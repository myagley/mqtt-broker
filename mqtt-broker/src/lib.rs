use std::sync::Arc;

use mqtt::proto::*;

mod broker;
mod connection;
mod error;
mod server;
mod session;

pub use crate::connection::ConnectionHandle;
pub use crate::error::{Error, ErrorKind};
pub use crate::server::Server;

#[derive(Clone, Debug)]
pub struct ClientId(Arc<String>);

impl ClientId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug)]
pub enum Event {
    Connect(Connect, ConnectionHandle),
    ConnAck(ConnAck),
    Gone,
    Unknown,
}

#[derive(Debug)]
pub struct Message {
    client_id: ClientId,
    event: Event,
}

impl Message {
    pub fn new(client_id: ClientId, event: Event) -> Self {
        Self { client_id, event }
    }

    pub fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    pub fn event(&self) -> &Event {
        &self.event
    }

    pub fn into_event(self) -> Event {
        self.event
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
