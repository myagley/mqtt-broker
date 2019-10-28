use std::collections::HashMap;

use failure::ResultExt;
use mqtt::proto;
use tracing::{debug, info, warn};

use crate::{ClientId, ConnectionHandle, Error, ErrorKind, Event, Message};

#[derive(Debug)]
pub struct Session {
    client_id: ClientId,
    handle: ConnectionHandle,
}

impl Session {
    fn new(client_id: ClientId, handle: ConnectionHandle) -> Self {
        Self { client_id, handle }
    }

    pub async fn send(&mut self, event: Event) -> Result<(), Error> {
        let message = Message::new(self.client_id.clone(), event);
        self.handle
            .send(message)
            .await
            .context(ErrorKind::SendConnectionMessage)?;
        Ok(())
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
        handle: ConnectionHandle,
    ) -> Result<proto::ConnAck, SessionError> {
        if let Some(session) = self.sessions.remove(&client_id) {
            if session.handle == handle {
                // [MQTT-3.1.0-2] - The Server MUST process a second CONNECT Packet
                // sent from a Client as a protocol violation and disconnect the Client.
                //
                // If the handles are equal, this is a second CONNECT packet on the
                // same physical connection. We need to treat this as a protocol
                // violation, move the session to offline, drop the connection, and return.

                warn!("CONNECT packet received on an already established connection, dropping connection due to protocol violation");
                Err(SessionError::ProtocolViolation(session))
            } else {
                // [MQTT-3.1.4-2] If the ClientId represents a Client already connected to the Server
                // then the Server MUST disconnect the existing Client.
                //
                // Send a DropConnection to the current handle.
                // Update the session to use the new handle.

                // TODO add session state for clean session

                info!(
                    "connection request for an in use client id ({}). closing previous connection",
                    client_id
                );

                let new_session = Session::new(client_id.clone(), handle);
                self.sessions.insert(client_id.clone(), new_session);

                let ack = proto::ConnAck {
                    session_present: false,
                    return_code: proto::ConnectReturnCode::Accepted,
                };

                Err(SessionError::DuplicateSession(session, ack))
            }
        } else {
            // No session present - create a new one.
            debug!("creating new session");
            let new_session = Session::new(client_id.clone(), handle);
            self.sessions.insert(client_id.clone(), new_session);

            let ack = proto::ConnAck {
                session_present: false,
                return_code: proto::ConnectReturnCode::Accepted,
            };

            Ok(ack)
        }
    }

    pub fn remove_session(&mut self, client_id: &ClientId) -> Option<Session> {
        self.sessions.remove(client_id)
    }

    pub async fn send(&mut self, client_id: &ClientId, event: Event) -> Result<(), Error> {
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or(Error::new(ErrorKind::NoSession.into()))?;
        session.send(event).await
    }
}
