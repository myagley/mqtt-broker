use std::collections::HashMap;
use std::time::{Duration, Instant};

use failure::ResultExt;
use mqtt::proto;
use tokio::clock;
use tracing::{debug, info, warn};

use crate::{ClientId, ConnReq, ConnectionHandle, Error, ErrorKind, Event, Message};

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
}

impl Connected {
    fn new(state: State, handle: ConnectionHandle) -> Self {
        Self { state, handle }
    }

    async fn send(&mut self, event: Event) -> Result<(), Error> {
        self.state.last_active = clock::now();

        let message = Message::new(self.state.client_id.clone(), event);
        self.handle
            .send(message)
            .await
            .context(ErrorKind::SendConnectionMessage)?;
        Ok(())
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
    Transient(Connected),
    Persistent(Connected),
    Disconnecting(ClientId, ConnectionHandle),
    Offline(Offline),
}

impl Session {
    fn new_transient(connreq: ConnReq) -> Self {
        let state = State::new(connreq.client_id().clone(), connreq.connect().keep_alive);
        let connected = Connected::new(state, connreq.into_handle());
        Session::Transient(connected)
    }

    fn new_persistent(connreq: ConnReq, state: State) -> Self {
        let connected = Connected::new(state, connreq.into_handle());
        Session::Persistent(connected)
    }

    pub async fn send(&mut self, event: Event) -> Result<(), Error> {
        match self {
            Session::Transient(ref mut connected) => connected.send(event).await,
            Session::Persistent(ref mut connected) => connected.send(event).await,
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

#[derive(Debug)]
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

    pub fn open_session(&mut self, connreq: ConnReq) -> Result<proto::ConnAck, SessionError> {
        let client_id = connreq.client_id().clone();

        match self.sessions.remove(&client_id) {
            Some(Session::Transient(current_connected)) => {
                self.open_session_connected(connreq, current_connected)
            }
            Some(Session::Persistent(current_connected)) => {
                self.open_session_connected(connreq, current_connected)
            }
            Some(Session::Offline(offline)) => {
                debug!("found an offline session for {}", client_id);

                let (new_session, session_present) = if let proto::ClientId::IdWithExistingSession(
                    _,
                ) = connreq.connect().client_id
                {
                    let new_session = Session::new_persistent(connreq, offline.state);
                    (new_session, true)
                } else {
                    let new_session = Session::new_transient(connreq);
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

                let new_session = if let proto::ClientId::IdWithExistingSession(_) =
                    connreq.connect().client_id
                {
                    let state = State::new(client_id.clone(), connreq.connect().keep_alive);
                    Session::new_persistent(connreq, state)
                } else {
                    Session::new_transient(connreq)
                };

                self.sessions.insert(client_id.clone(), new_session);

                let ack = proto::ConnAck {
                    session_present: false,
                    return_code: proto::ConnectReturnCode::Accepted,
                };

                Ok(ack)
            }
        }
    }

    fn open_session_connected(
        &mut self,
        connreq: ConnReq,
        current_connected: Connected,
    ) -> Result<proto::ConnAck, SessionError> {
        if current_connected.handle == *connreq.handle() {
            // [MQTT-3.1.0-2] - The Server MUST process a second CONNECT Packet
            // sent from a Client as a protocol violation and disconnect the Client.
            //
            // If the handles are equal, this is a second CONNECT packet on the
            // same physical connection. We need to treat this as a protocol
            // violation, move the session to offline, drop the connection, and return.

            warn!("CONNECT packet received on an already established connection, dropping connection due to protocol violation");
            Err(SessionError::ProtocolViolation(Session::Disconnecting(
                connreq.client_id().clone(),
                current_connected.handle,
            )))
        } else {
            // [MQTT-3.1.4-2] If the ClientId represents a Client already connected to the Server
            // then the Server MUST disconnect the existing Client.
            //
            // Send a DropConnection to the current handle.
            // Update the session to use the new handle.

            info!(
                "connection request for an in use client id ({}). closing previous connection",
                connreq.client_id()
            );

            let client_id = connreq.client_id().clone();
            let (new_session, old_session, session_present) =
                if let proto::ClientId::IdWithExistingSession(_) = connreq.connect().client_id {
                    let old_session = Session::Disconnecting(
                        connreq.client_id().clone(),
                        current_connected.handle,
                    );
                    let new_session = Session::new_persistent(connreq, current_connected.state);
                    (new_session, old_session, true)
                } else {
                    let old_session = Session::Disconnecting(
                        connreq.client_id().clone(),
                        current_connected.handle,
                    );
                    let new_session = Session::new_transient(connreq);
                    (new_session, old_session, false)
                };

            self.sessions.insert(client_id, new_session);
            let ack = proto::ConnAck {
                session_present,
                return_code: proto::ConnectReturnCode::Accepted,
            };

            Err(SessionError::DuplicateSession(old_session, ack))
        }
    }

    pub fn close_session(&mut self, client_id: &ClientId) -> Option<Session> {
        match self.sessions.remove(client_id) {
            Some(Session::Transient(connected)) => {
                Some(Session::Disconnecting(client_id.clone(), connected.handle))
            }
            Some(Session::Persistent(connected)) => {
                // Move a persistent session into the offline state
                // Return a disconnecting session to allow a disconnect
                // to be sent on the connection

                let new_session = Session::Offline(Offline::new(connected.state));
                self.sessions.insert(client_id.clone(), new_session);
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

#[cfg(test)]
mod tests {
    use super::*;

    use matches::assert_matches;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    fn connection_handle() -> ConnectionHandle {
        let id = Uuid::new_v4();
        let (tx1, _rx1) = mpsc::channel(128);
        ConnectionHandle::new(id, tx1)
    }

    fn transient_connect(id: String) -> proto::Connect {
        proto::Connect {
            username: None,
            password: None,
            will: None,
            client_id: proto::ClientId::IdWithCleanSession(id),
            keep_alive: Default::default(),
            protocol_name: "MQTT".to_string(),
            protocol_level: 0x4,
        }
    }

    fn persistent_connect(id: String) -> proto::Connect {
        proto::Connect {
            username: None,
            password: None,
            will: None,
            client_id: proto::ClientId::IdWithExistingSession(id),
            keep_alive: Default::default(),
            protocol_name: "MQTT".to_string(),
            protocol_level: 0x4,
        }
    }

    #[test]
    fn test_add_session_empty_transient() {
        let id = "id1".to_string();
        let mut manager = SessionManager::new();
        let client_id = ClientId::from(id.clone());
        let connect = transient_connect(id.clone());
        let handle = connection_handle();
        let req = ConnReq::new(client_id.clone(), connect, handle);

        manager.open_session(req).unwrap();

        // check new session
        assert_eq!(1, manager.sessions.len());
        assert_matches!(manager.sessions[&client_id], Session::Transient(_));

        // close session and check behavior
        let old_session = manager.close_session(&client_id);
        assert_matches!(old_session, Some(Session::Disconnecting(_, _)));
        assert_eq!(0, manager.sessions.len());
    }

    #[test]
    fn test_add_session_empty_persistent() {
        let id = "id1".to_string();
        let mut manager = SessionManager::new();
        let client_id = ClientId::from(id.clone());
        let connect = persistent_connect(id.clone());
        let handle = connection_handle();
        let req = ConnReq::new(client_id.clone(), connect, handle);

        manager.open_session(req).unwrap();

        assert_eq!(1, manager.sessions.len());
        assert_matches!(manager.sessions[&client_id], Session::Persistent(_));

        // close session and check behavior
        let old_session = manager.close_session(&client_id);
        assert_matches!(old_session, Some(Session::Disconnecting(_, _)));
        assert_eq!(1, manager.sessions.len());
        assert_matches!(manager.sessions[&client_id], Session::Offline(_));
    }

    #[test]
    fn test_add_session_same_connection() {
        let id = "id1".to_string();
        let mut manager = SessionManager::new();
        let client_id = ClientId::from(id.clone());
        let connect1 = transient_connect(id.clone());
        let connect2 = transient_connect(id.clone());
        let id = Uuid::new_v4();
        let (tx1, _rx1) = mpsc::channel(128);
        let handle1 = ConnectionHandle::new(id.clone(), tx1.clone());
        let handle2 = ConnectionHandle::new(id, tx1);

        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);
        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);

        manager.open_session(req1).unwrap();
        assert_eq!(1, manager.sessions.len());

        let result = manager.open_session(req2);
        assert_matches!(result, Err(SessionError::ProtocolViolation(_)));
        assert_eq!(0, manager.sessions.len());
    }

    #[test]
    fn test_add_session_different_connection_transient_then_transient() {
        let id = "id1".to_string();
        let mut manager = SessionManager::new();
        let client_id = ClientId::from(id.clone());
        let connect1 = transient_connect(id.clone());
        let connect2 = transient_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);

        manager.open_session(req1).unwrap();
        assert_eq!(1, manager.sessions.len());

        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);
        let result = manager.open_session(req2);
        assert_matches!(result, Err(SessionError::DuplicateSession(_, _)));
        assert_matches!(manager.sessions[&client_id], Session::Transient(_));
        assert_eq!(1, manager.sessions.len());
    }

    #[test]
    fn test_add_session_different_connection_transient_then_persistent() {
        let id = "id1".to_string();
        let mut manager = SessionManager::new();
        let client_id = ClientId::from(id.clone());
        let connect1 = transient_connect(id.clone());
        let connect2 = persistent_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);

        manager.open_session(req1).unwrap();
        assert_eq!(1, manager.sessions.len());

        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);
        let result = manager.open_session(req2);
        assert_matches!(result, Err(SessionError::DuplicateSession(_, _)));
        assert_matches!(manager.sessions[&client_id], Session::Persistent(_));
        assert_eq!(1, manager.sessions.len());
    }

    #[test]
    fn test_add_session_different_connection_persistent_then_transient() {
        let id = "id1".to_string();
        let mut manager = SessionManager::new();
        let client_id = ClientId::from(id.clone());
        let connect1 = persistent_connect(id.clone());
        let connect2 = transient_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);

        manager.open_session(req1).unwrap();
        assert_eq!(1, manager.sessions.len());

        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);
        let result = manager.open_session(req2);
        assert_matches!(result, Err(SessionError::DuplicateSession(_, _)));
        assert_matches!(manager.sessions[&client_id], Session::Transient(_));
        assert_eq!(1, manager.sessions.len());
    }

    #[test]
    fn test_add_session_different_connection_persistent_then_persistent() {
        let id = "id1".to_string();
        let mut manager = SessionManager::new();
        let client_id = ClientId::from(id.clone());
        let connect1 = persistent_connect(id.clone());
        let connect2 = persistent_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);
        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);

        manager.open_session(req1).unwrap();
        assert_eq!(1, manager.sessions.len());

        let result = manager.open_session(req2);
        assert_matches!(result, Err(SessionError::DuplicateSession(_, _)));
        assert_matches!(manager.sessions[&client_id], Session::Persistent(_));
        assert_eq!(1, manager.sessions.len());
    }

    #[test]
    fn test_add_session_offline_persistent() {
        let id = "id1".to_string();
        let mut manager = SessionManager::new();
        let client_id = ClientId::from(id.clone());
        let connect1 = persistent_connect(id.clone());
        let connect2 = persistent_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);
        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);

        manager.open_session(req1).unwrap();

        assert_eq!(1, manager.sessions.len());
        assert_matches!(manager.sessions[&client_id], Session::Persistent(_));

        // close session and check behavior
        let old_session = manager.close_session(&client_id);
        assert_matches!(old_session, Some(Session::Disconnecting(_, _)));
        assert_eq!(1, manager.sessions.len());
        assert_matches!(manager.sessions[&client_id], Session::Offline(_));

        // Reopen session
        manager.open_session(req2).unwrap();

        assert_eq!(1, manager.sessions.len());
        assert_matches!(manager.sessions[&client_id], Session::Persistent(_));
    }

    #[test]
    fn test_add_session_offline_transient() {
        let id = "id1".to_string();
        let mut manager = SessionManager::new();
        let client_id = ClientId::from(id.clone());
        let connect1 = persistent_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);

        manager.open_session(req1).unwrap();

        assert_eq!(1, manager.sessions.len());
        assert_matches!(manager.sessions[&client_id], Session::Persistent(_));

        // close session and check behavior
        let old_session = manager.close_session(&client_id);
        assert_matches!(old_session, Some(Session::Disconnecting(_, _)));
        assert_eq!(1, manager.sessions.len());
        assert_matches!(manager.sessions[&client_id], Session::Offline(_));

        // Reopen session
        let connect2 = transient_connect(id.clone());
        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);
        manager.open_session(req2).unwrap();

        assert_eq!(1, manager.sessions.len());
        assert_matches!(manager.sessions[&client_id], Session::Transient(_));
    }
}
