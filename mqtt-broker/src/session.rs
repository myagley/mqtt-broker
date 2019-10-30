use std::time::{Duration, Instant};

use failure::ResultExt;
use tokio::clock;

use crate::{ClientId, ConnReq, ConnectionHandle, Error, ErrorKind, Event, Message};

#[derive(Debug)]
pub struct ConnectedSession {
    state: SessionState,
    handle: ConnectionHandle,
}

impl ConnectedSession {
    fn new(state: SessionState, handle: ConnectionHandle) -> Self {
        Self { state, handle }
    }

    pub fn handle(&self) -> &ConnectionHandle {
        &self.handle
    }

    pub fn into_handle(self) -> ConnectionHandle {
        self.handle
    }

    pub fn into_parts(self) -> (SessionState, ConnectionHandle) {
        (self.state, self.handle)
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
pub struct OfflineSession {
    state: SessionState,
}

impl OfflineSession {
    fn new(state: SessionState) -> Self {
        Self { state }
    }

    pub fn into_state(self) -> SessionState {
        self.state
    }
}

#[derive(Debug)]
pub struct SessionState {
    client_id: ClientId,
    keep_alive: Duration,
    last_active: Instant,
}

impl SessionState {
    pub fn new(client_id: ClientId, connreq: &ConnReq) -> Self {
        Self {
            client_id,
            keep_alive: connreq.connect().keep_alive,
            last_active: clock::now(),
        }
    }
}

#[derive(Debug)]
pub enum Session {
    Transient(ConnectedSession),
    Persistent(ConnectedSession),
    Disconnecting(ClientId, ConnectionHandle),
    Offline(OfflineSession),
}

impl Session {
    pub fn new_transient(connreq: ConnReq) -> Self {
        let state = SessionState::new(connreq.client_id().clone(), &connreq);
        let connected = ConnectedSession::new(state, connreq.into_handle());
        Session::Transient(connected)
    }

    pub fn new_persistent(connreq: ConnReq, state: SessionState) -> Self {
        let connected = ConnectedSession::new(state, connreq.into_handle());
        Session::Persistent(connected)
    }

    pub fn new_offline(state: SessionState) -> Self {
        let offline = OfflineSession::new(state);
        Session::Offline(offline)
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

#[cfg(test)]
mod tests {}
