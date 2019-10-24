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
use uuid::Uuid;

use crate::broker::BrokerHandle;
use crate::{ClientId, Error, ErrorKind, Event, Message};

#[derive(Clone, Debug)]
pub struct ConnectionHandle(Sender<Message>);

impl ConnectionHandle {
    pub async fn send(&mut self, message: Message) -> Result<(), Error> {
        self.0
            .send(message)
            .await
            .context(ErrorKind::SendConnectionMessage)?;
        Ok(())
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
            let (sender, events) = mpsc::channel(128);
            let connection_handle = ConnectionHandle(sender);

            // Send the connect event to the broker
            let client_id = client_id(&connect.client_id);
            let event = Event::Connect(connect, connection_handle.clone());
            let message = Message::new(client_id.clone(), event);
            broker_handle.send(message).await?;

            // Start up the processing tasks
            let incoming_task = incoming_task(
                client_id.clone(),
                incoming,
                broker_handle,
                connection_handle,
            );
            let outgoing_task = outgoing_task(events, outgoing);
            pin_mut!(incoming_task);
            pin_mut!(outgoing_task);

            match select(incoming_task, outgoing_task).await {
                Either::Left((res, _)) => println!("incoming finished with: {:?}", res),
                Either::Right((res, _)) => println!("outgoing finished with: {:?}", res),
            }

            println!("closing connection");
            Ok(())
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
    while let Some(maybe_packet) = incoming.next().await {
        match maybe_packet {
            Ok(packet) => {
                println!("incoming: {:?}", packet);
                let event = match packet {
                    Packet::Connect(connect) => Event::Connect(connect, connection.clone()),
                    _ => Event::Unknown,
                };

                let message = Message::new(client_id.clone(), event);
                broker.send(message).await?;
            }
            Err(e) => {
                println!("error from stream: {:?}", e);
                return Err(e.context(ErrorKind::DecodePacket).into());
            }
        }
    }
    println!("no more frames");
    Ok(())
}

async fn outgoing_task<S>(mut messages: Receiver<Message>, mut outgoing: S) -> Result<(), Error>
where
    S: Sink<Packet, Error = EncodeError> + Unpin,
{
    while let Some(message) = messages.recv().await {
        println!("outgoing: {:?}", message);
        match message.into_event() {
            Event::ConnAck(connack) => outgoing
                .send(Packet::ConnAck(connack))
                .await
                .context(ErrorKind::EncodePacket)?,
            _ => (),
        }
    }
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
