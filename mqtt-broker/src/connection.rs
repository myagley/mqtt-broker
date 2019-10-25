use std::fmt;
use std::sync::Arc;

use failure::{Fail, ResultExt};
use futures_util::future::{select, Either};
use futures_util::pin_mut;
use futures_util::sink::{Sink, SinkExt};
use futures_util::stream::{Stream, StreamExt};
use mqtt::proto::{self, DecodeError, EncodeError, Packet, PacketCodec};
use tokio::codec::Framed;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, info, span, warn, Level};
use tracing_futures::Instrument;
use uuid::Uuid;

use crate::broker::BrokerHandle;
use crate::{ClientId, Error, ErrorKind, Event, Message};

#[derive(Clone, Debug)]
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
    let codec = Framed::new(io, PacketCodec::default());
    let (outgoing, mut incoming) = codec.split();

    // [MQTT-3.1.0-1] - After a Network Connection is established by a Client to a Server,
    // the first Packet sent from the Client to the Server MUST be a CONNECT Packet.
    //
    // We need to handle the first CONNECT packet here (instead of in the broker state machine)
    // so that we can get and cache the client_id for use with other packets.
    // The broker state machine will also have to handle not receiving a connect packet first
    // to keep the state machine correct.

    match incoming.next().await {
        Some(Ok(Packet::Connect(connect))) => {
            let client_id = client_id(&connect.client_id);
            let (sender, events) = mpsc::channel(128);
            let connection_handle = ConnectionHandle::from_sender(sender);
            let span = span!(Level::INFO, "connection", client_id=%client_id, connection=%connection_handle);

            // async block to attach instrumentation context
            async {
                info!("new client connection");

                let event = Event::Connect(connect, connection_handle.clone());
                let message = Message::new(client_id.clone(), event);
                broker_handle.send(message).await?;

                // Start up the processing tasks
                let incoming_task =
                    incoming_task(client_id.clone(), incoming, broker_handle.clone(), connection_handle);
                let outgoing_task = outgoing_task(events, outgoing);
                pin_mut!(incoming_task);
                pin_mut!(outgoing_task);

                match select(incoming_task, outgoing_task).await {
                    Either::Left((Ok(()), out)) => {
                        debug!("incoming_task finished with ok. waiting for outgoing_task to complete...");
                        out.await?;
                        debug!("outgoing_task completed.");
                    }
                    Either::Left((Err(_e), out)) => {
                        // incoming packet stream completed with an error
                        // send a DropConnection request to the broker and wait for the outgoing
                        // task to drain
                        debug!("incoming_task finished with an error. sending drop connection request to broker");
                        let msg = Message::new(client_id.clone(), Event::DropConnection);
                        broker_handle.send(msg).await?;

                        debug!("waiting for outgoing_task to complete...");
                        out.await?;
                        debug!("outgoing_task completed.");
                    }
                    Either::Right((Ok(()), _)) => debug!("outgoing finished with ok"),
                    Either::Right((Err(_e), _)) => {
                        // outgoing task failed with an error.
                        // Notify the broker that the connection is gone and close the connection

                        debug!("outgoing_task finished with an error. notifying the broker to remove the connection");
                        let msg = Message::new(client_id.clone(), Event::CloseSession);
                        broker_handle.send(msg).await?;

                        // TODO: should probably return the message receiver and drain it so that
                        // there aren't errors in the broker
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
    connection: ConnectionHandle,
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
                    Packet::Connect(connect) => Event::Connect(connect, connection.clone()),
                    Packet::Disconnect(disconnect) => {
                        let event = Event::Disconnect(disconnect);
                        let message = Message::new(client_id.clone(), event);
                        broker.send(message).await?;
                        debug!("disconnect received. shutting down receive side of connection");
                        return Ok(());
                    }
                    _ => Event::Unknown,
                };

                let message = Message::new(client_id.clone(), event);
                broker.send(message).await?;
            }
            Err(e) => {
                warn!(message="error from stream", error=%e);
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

async fn outgoing_task<S>(mut messages: Receiver<Message>, mut outgoing: S) -> Result<(), Error>
where
    S: Sink<Packet, Error = EncodeError> + Unpin,
{
    debug!("outgoing_task start");
    while let Some(message) = messages.recv().await {
        debug!("outgoing: {:?}", message);
        match message.into_event() {
            Event::ConnAck(connack) => {
                outgoing
                    .send(Packet::ConnAck(connack))
                    .await
                    .context(ErrorKind::EncodePacket)?;
                debug!("sent packet to client");
            }
            Event::Disconnect(_) => {
                debug!("asked to disconnect. outgoing_task completing...");
                return Ok(());
            }
            Event::DropConnection => {
                debug!("asked to drop connection. outgoing_task completing...");
                return Ok(());
            }
            _ => (),
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
