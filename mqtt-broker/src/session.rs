use std::collections::HashMap;
use std::time::{Duration, Instant};

use failure::ResultExt;
use matches::matches;
use mqtt::proto;
use tokio::clock;
use tracing::{debug, info, warn};

use crate::{ClientId, ConnectionHandle, Error, ErrorKind, Event, Message};

#[derive(Debug)]
pub struct State {
    client_id: ClientId,
    keep_alive: Duration,
    last_active: Instant,
}

#[derive(Debug)]
pub struct Connected {
    state: State,
    handle: ConnectionHandle,
    persistent: bool,
}

impl Connected {
    fn new(client_id: ClientId, connect: &proto::Connect, handle: ConnectionHandle) -> Self {
        let persistent = matches!(connect.client_id, proto::ClientId::IdWithExistingSession(_));
        Self {
            state: State::new(client_id, connect.keep_alive),
            handle,
            persistent,
        }
    }
}

#[derive(Debug)]
pub struct Offline {
    state: State,
}

impl Offline {
    fn new(state: State) -> Self {
        Self { state }
    }
}

impl State {
    fn new(client_id: ClientId, keep_alive: Duration) -> Self {
        Self {
            client_id,
            keep_alive,
            last_active: clock::now(),
        }
    }
}

#[derive(Debug)]
pub enum Session {
    Connected(Connected),
    Disconnecting(ClientId, ConnectionHandle),
    Offline(Offline),
}

impl Session {
    fn new_connected(state: State, handle: ConnectionHandle, persistent: bool) -> Self {
        let connected = Connected {
            state,
            handle,
            persistent,
        };
        Session::Connected(connected)
    }

    pub async fn send(&mut self, event: Event) -> Result<(), Error> {
        match self {
            Session::Connected(ref mut connected) => {
                connected.state.last_active = clock::now();

                let message = Message::new(connected.state.client_id.clone(), event);
                connected
                    .handle
                    .send(message)
                    .await
                    .context(ErrorKind::SendConnectionMessage)?;
                Ok(())
            }
            Session::Disconnecting(ref client_id, ref mut handle) => {
                let message = Message::new(client_id.clone(), event);
                handle
                    .send(message)
                    .await
                    .context(ErrorKind::SendConnectionMessage)?;
                Ok(())
            }
            _ => Err(ErrorKind::SessionOffline.into()),
        }
    }
}

pub enum SessionError {
    ProtocolViolation(Session),
    DuplicateSession(Session, proto::ConnAck),
}

pub struct SessionManager {
    sessions: HashMap<ClientId, Session>,
}

impl SessionManager {
    pub fn new() -> Self {
        SessionManager {
            sessions: HashMap::new(),
        }
    }

    pub fn add_session(
        &mut self,
        client_id: ClientId,
        connect: &proto::Connect,
        handle: ConnectionHandle,
    ) -> Result<proto::ConnAck, SessionError> {
        match self.sessions.remove(&client_id) {
            Some(Session::Connected(connected)) => {
                if connected.handle == handle {
                    // [MQTT-3.1.0-2] - The Server MUST process a second CONNECT Packet
                    // sent from a Client as a protocol violation and disconnect the Client.
                    //
                    // If the handles are equal, this is a second CONNECT packet on the
                    // same physical connection. We need to treat this as a protocol
                    // violation, move the session to offline, drop the connection, and return.

                    warn!("CONNECT packet received on an already established connection, dropping connection due to protocol violation");
                    Err(SessionError::ProtocolViolation(Session::Disconnecting(
                        client_id,
                        connected.handle,
                    )))
                } else {
                    // [MQTT-3.1.4-2] If the ClientId represents a Client already connected to the Server
                    // then the Server MUST disconnect the existing Client.
                    //
                    // Send a DropConnection to the current handle.
                    // Update the session to use the new handle.

                    info!(
                        "connection request for an in use client id ({}). closing previous connection",
                        client_id
                    );

                    let (old_session, session_present) =
                        if let proto::ClientId::IdWithExistingSession(_) = connect.client_id {
                            let old_session =
                                Session::Disconnecting(client_id.clone(), connected.handle);
                            let new_session = Session::new_connected(connected.state, handle, true);
                            self.sessions.insert(client_id, new_session);
                            (old_session, true)
                        } else {
                            let old_session =
                                Session::Disconnecting(client_id.clone(), connected.handle);
                            let new_session = Session::Connected(Connected::new(
                                client_id.clone(),
                                connect,
                                handle,
                            ));
                            self.sessions.insert(client_id, new_session);
                            (old_session, false)
                        };

                    let ack = proto::ConnAck {
                        session_present,
                        return_code: proto::ConnectReturnCode::Accepted,
                    };

                    Err(SessionError::DuplicateSession(old_session, ack))
                }
            }
            Some(Session::Offline(offline)) => {
                debug!("found an offline session for {}", client_id);

                let (new_session, session_present) =
                    if let proto::ClientId::IdWithExistingSession(_) = connect.client_id {
                        let new_session = Session::new_connected(offline.state, handle, true);
                        (new_session, true)
                    } else {
                        let new_session =
                            Session::Connected(Connected::new(client_id.clone(), connect, handle));
                        (new_session, false)
                    };

                self.sessions.insert(client_id, new_session);

                let ack = proto::ConnAck {
                    session_present,
                    return_code: proto::ConnectReturnCode::Accepted,
                };

                Ok(ack)
            }
            Some(Session::Disconnecting(client_id, handle)) => Err(
                SessionError::ProtocolViolation(Session::Disconnecting(client_id, handle)),
            ),
            None => {
                // No session present - create a new one.
                debug!("creating new session");
                let new_session =
                    Session::Connected(Connected::new(client_id.clone(), connect, handle));
                self.sessions.insert(client_id.clone(), new_session);

                let ack = proto::ConnAck {
                    session_present: false,
                    return_code: proto::ConnectReturnCode::Accepted,
                };

                Ok(ack)
            }
        }
    }

    pub fn close_session(&mut self, client_id: &ClientId) -> Option<Session> {
        match self.sessions.remove(client_id) {
            Some(Session::Connected(connected)) => {
                // Move a persistent session into the offline state
                // Return a disconnecting session to allow a disconnect
                // to be sent on the connection

                if connected.persistent == true {
                    let new_session = Session::Offline(Offline::new(connected.state));
                    self.sessions.insert(client_id.clone(), new_session);
                }

                Some(Session::Disconnecting(client_id.clone(), connected.handle))
            }
            maybe_session => maybe_session,
        }
    }

    pub async fn send(&mut self, client_id: &ClientId, event: Event) -> Result<(), Error> {
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or(Error::new(ErrorKind::NoSession.into()))?;
        session.send(event).await
    }
}
