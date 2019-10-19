use mqtt::proto::*;

mod broker;
mod connection;
mod error;
mod server;
mod session;

pub use crate::connection::ConnectionHandle;
pub use crate::error::{Error, ErrorKind};
pub use crate::server::Server;

#[derive(Debug)]
pub enum Event {
    Connect(Connect, ConnectionHandle),
    ConnAck(ConnAck),
    Gone,
    Unknown,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
