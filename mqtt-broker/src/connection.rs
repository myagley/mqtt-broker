use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use failure::{Fail, ResultExt};
use futures_util::future::{select, Either};
use futures_util::pin_mut;
use futures_util::sink::{Sink, SinkExt};
use futures_util::stream::{Stream, StreamExt};
use lazy_static::lazy_static;
use mqtt::proto::{self, DecodeError, EncodeError, Packet, PacketCodec};
use tokio::codec::Framed;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_io_timeout::TimeoutStream;
use tracing::{debug, info, span, trace, warn, Level};
use tracing_futures::Instrument;
use uuid::Uuid;

use crate::broker::BrokerHandle;
use crate::{ClientId, ConnReq, Error, ErrorKind, Event, Message};

lazy_static! {
    static ref DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
}

const KEEPALIVE_MULT: f32 = 1.5;

/// Allows sending events to a connection.
///
/// It is important that this struct doesn't implement Clone,
/// as the lifecycle management depends on there only being
/// one sender.
#[derive(Debug)]
pub struct ConnectionHandle {
    id: Uuid,
    sender: Sender<Message>,
}

impl ConnectionHandle {
    pub(crate) fn new(id: Uuid, sender: Sender<Message>) -> Self {
        Self { id, sender }
    }

    pub fn from_sender(sender: Sender<Message>) -> Self {
        Self::new(Uuid::new_v4(), sender)
    }

    pub async fn send(&mut self, message: Message) -> Result<(), Error> {
        self.sender
            .send(message)
            .await
            .context(ErrorKind::SendConnectionMessage)?;
        Ok(())
    }
}

impl PartialEq for ConnectionHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl fmt::Display for ConnectionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

/// Handles packet processing for a single connection.
///
/// Receives a source of packets and a handle to the Broker.
/// Starts two tasks (sending and receiving)
pub async fn process<I>(io: I, mut broker_handle: BrokerHandle) -> Result<(), Error>
where
    I: AsyncRead + AsyncWrite + Unpin,
{
    let mut timeout = TimeoutStream::new(io);
    timeout.set_read_timeout(Some(*DEFAULT_TIMEOUT));
    timeout.set_write_timeout(Some(*DEFAULT_TIMEOUT));

    let mut codec = Framed::new(timeout, PacketCodec::default());

    // [MQTT-3.1.0-1] - After a Network Connection is established by a Client to a Server,
    // the first Packet sent from the Client to the Server MUST be a CONNECT Packet.
    //
    // We need to handle the first CONNECT packet here (instead of in the broker state machine)
    // so that we can get and cache the client_id for use with other packets.
    // The broker state machine will also have to handle not receiving a connect packet first
    // to keep the state machine correct.

    match codec.next().await {
        Some(Ok(Packet::Connect(connect))) => {
            let client_id = client_id(&connect.client_id);
            let (sender, events) = mpsc::channel(128);
            let connection_handle = ConnectionHandle::from_sender(sender);
            let span = span!(Level::INFO, "connection", client_id=%client_id, connection=%connection_handle);

            // async block to attach instrumentation context
            async {
                info!("new client connection");

                // [MQTT-3.1.2-24] - If the Keep Alive value is non-zero and
                // the Server does not receive a Control Packet from the
                // Client within one and a half times the Keep Alive time
                // period, it MUST disconnect the Network Connection to the
                // Client as if the network had failed.
                let keep_alive = connect.keep_alive.mul_f32(KEEPALIVE_MULT);
                if keep_alive == Duration::from_secs(0) {
                    debug!("received 0 length keepalive from client. disabling keepalive timeout");
                    codec.get_mut().set_read_timeout(None);
                } else {
                    debug!("using keepalive timeout of {:?}", keep_alive);
                    codec.get_mut().set_read_timeout(Some(keep_alive));
                }

                let (outgoing, incoming) = codec.split();

                let req = ConnReq::new(client_id.clone(), connect, connection_handle);
                let event = Event::ConnReq(req);
                let message = Message::new(client_id.clone(), event);
                broker_handle.send(message).await?;

                // Start up the processing tasks
                let incoming_task =
                    incoming_task(client_id.clone(), incoming, broker_handle.clone());
                let outgoing_task = outgoing_task(events, outgoing);
                pin_mut!(incoming_task);
                pin_mut!(outgoing_task);

                match select(incoming_task, outgoing_task).await {
                    Either::Left((Ok(()), out)) => {
                        debug!("incoming_task finished with ok. waiting for outgoing_task to complete...");
                        out.await.map_err(|(_, e)| e)?;
                        debug!("outgoing_task completed.");
                    }
                    Either::Left((Err(e), out)) => {
                        // incoming packet stream completed with an error
                        // send a DropConnection request to the broker and wait for the outgoing
                        // task to drain
                        debug!(message = "incoming_task finished with an error. sending drop connection request to broker", error=%e);
                        let msg = Message::new(client_id.clone(), Event::DropConnection);
                        broker_handle.send(msg).await?;

                        debug!("waiting for outgoing_task to complete...");
                        out.await.map_err(|(_, e)| e)?;
                        debug!("outgoing_task completed.");
                    }
                    Either::Right((Ok(()), inc)) => {
                        drop(inc);
                        debug!("outgoing finished with ok")
                    }
                    Either::Right((Err((mut recv, e)), inc)) => {
                        // outgoing task failed with an error.
                        // drop the incoming packet processing
                        // Notify the broker that the connection is gone, drain the receiver, and
                        // close the connection

                        drop(inc);

                        debug!(message = "outgoing_task finished with an error. notifying the broker to remove the connection", %e);
                        let msg = Message::new(client_id.clone(), Event::CloseSession);
                        broker_handle.send(msg).await?;

                        debug!("draining message receiver for connection...");
                        while let Some(message) = recv.recv().await {
                            trace!("dropping {:?}", message);
                        }
                        debug!("message receiver draining completed.");
                    }
                }

                info!("closing connection");
                Ok(())
            }
                .instrument(span)
                .await
        }
        Some(Ok(packet)) => Err(ErrorKind::NoConnect(packet).into()),
        Some(Err(e)) => Err(e.context(ErrorKind::DecodePacket).into()),
        None => Err(ErrorKind::NoPackets.into()),
    }
}

async fn incoming_task<S>(
    client_id: ClientId,
    mut incoming: S,
    mut broker: BrokerHandle,
) -> Result<(), Error>
where
    S: Stream<Item = Result<Packet, DecodeError>> + Unpin,
{
    debug!("incoming_task start");
    while let Some(maybe_packet) = incoming.next().await {
        match maybe_packet {
            Ok(packet) => {
                debug!("incoming: {:?}", packet);
                let event = match packet {
                    Packet::Connect(_) => {
                        // [MQTT-3.1.0-2] - The Server MUST process a second CONNECT Packet
                        // sent from a Client as a protocol violation and disconnect the Client.

                        warn!("CONNECT packet received on an already established connection, dropping connection due to protocol violation");
                        return Err(Error::from(ErrorKind::ProtocolViolation));
                    }
                    Packet::Disconnect(disconnect) => {
                        let event = Event::Disconnect(disconnect);
                        let message = Message::new(client_id.clone(), event);
                        broker.send(message).await?;
                        debug!("disconnect received. shutting down receive side of connection");
                        return Ok(());
                    }
                    Packet::PingReq(ping) => Event::PingReq(ping),
                    _ => Event::Unknown,
                };

                let message = Message::new(client_id.clone(), event);
                broker.send(message).await?;
            }
            Err(e) => {
                warn!(message="error occurred while reading from connection", error=%e);
                return Err(e.context(ErrorKind::DecodePacket).into());
            }
        }
    }

    debug!("no more packets. sending DropConnection to broker.");
    let message = Message::new(client_id.clone(), Event::DropConnection);
    broker.send(message).await?;
    debug!("incoming_task completing...");
    Ok(())
}

async fn outgoing_task<S>(
    mut messages: Receiver<Message>,
    mut outgoing: S,
) -> Result<(), (Receiver<Message>, Error)>
where
    S: Sink<Packet, Error = EncodeError> + Unpin,
{
    debug!("outgoing_task start");
    while let Some(message) = messages.recv().await {
        debug!("outgoing: {:?}", message);
        let maybe_packet = match message.into_event() {
            Event::ConnReq(_) => None,
            Event::ConnAck(connack) => Some(Packet::ConnAck(connack)),
            Event::Disconnect(_) => {
                debug!("asked to disconnect. outgoing_task completing...");
                return Ok(());
            }
            Event::DropConnection => {
                debug!("asked to drop connection. outgoing_task completing...");
                return Ok(());
            }
            Event::CloseSession => None,
            Event::PingReq(_) => None,
            Event::PingResp(response) => Some(Packet::PingResp(response)),
            Event::Unknown => None,
        };

        if let Some(packet) = maybe_packet {
            let result = outgoing.send(packet).await.context(ErrorKind::EncodePacket);

            if let Err(e) = result {
                warn!(message = "error occurred while writing to connection", error=%e);
                return Err((messages, e.into()));
            }
        }
    }
    debug!("outgoing_task completing...");
    Ok(())
}

fn client_id(client_id: &proto::ClientId) -> ClientId {
    let id = match client_id {
        proto::ClientId::ServerGenerated => Uuid::new_v4().to_string(),
        proto::ClientId::IdWithCleanSession(ref id) => id.to_owned(),
        proto::ClientId::IdWithExistingSession(ref id) => id.to_owned(),
    };
    ClientId(Arc::new(id))
}
