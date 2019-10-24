use std::fmt;

use failure::{Backtrace, Context, Fail};
use mqtt::proto::Packet;

#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>,
}

#[derive(Debug, Fail)]
pub enum ErrorKind {
    #[fail(display = "An error occurred trying to connect.")]
    Connect,

    #[fail(display = "An error occurred sending a message to the broker.")]
    SendBrokerMessage,

    #[fail(display = "An error occurred sending a message to a connection.")]
    SendConnectionMessage,

    #[fail(display = "An error occurred binding the server's listening socket.")]
    BindServer,

    #[fail(display = "An error occurred decoding a packet.")]
    DecodePacket,

    #[fail(display = "An error occurred encoding a packet.")]
    EncodePacket,

    #[fail(
        display = "Expected CONNECT packet as first packet, received somethig {:?}",
        _0
    )]
    NoConnect(Packet),

    #[fail(display = "Connection closed before any packets received.")]
    NoPackets,
}

impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl Error {
    pub fn new(inner: Context<ErrorKind>) -> Self {
        Error { inner }
    }

    pub fn kind(&self) -> &ErrorKind {
        self.inner.get_context()
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Error {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<ErrorKind>> for Error {
    fn from(inner: Context<ErrorKind>) -> Self {
        Error { inner }
    }
}
