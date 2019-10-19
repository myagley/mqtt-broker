use failure::{Fail, ResultExt};
use futures_util::future::{select, Either};
use futures_util::pin_mut;
use futures_util::sink::{Sink, SinkExt};
use futures_util::stream::{Stream, StreamExt};
use mqtt::proto::{DecodeError, EncodeError, Packet, PacketCodec};
use tokio::codec::Framed;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::broker::BrokerHandle;
use crate::{Error, ErrorKind, Event};

#[derive(Clone, Debug)]
pub struct ConnectionHandle(Sender<Event>);

impl ConnectionHandle {
    pub async fn send(&mut self, event: Event) -> Result<(), Error> {
        self.0
            .send(event)
            .await
            .context(ErrorKind::SendConnectionEvent)?;
        Ok(())
    }
}

pub async fn process<I>(io: I, broker_handle: BrokerHandle) -> Result<(), Error>
where
    I: AsyncRead + AsyncWrite + Unpin,
{
    let codec = Framed::new(io, PacketCodec::default());
    let (outgoing, incoming) = codec.split();

    let (sender, events) = mpsc::channel(128);
    let connection_handle = ConnectionHandle(sender);

    let incoming = incoming_task(incoming, broker_handle, connection_handle);
    let outgoing = outgoing_task(events, outgoing);
    pin_mut!(incoming);
    pin_mut!(outgoing);

    match select(incoming, outgoing).await {
        Either::Left((res, _)) => println!("incoming finished with: {:?}", res),
        Either::Right((res, _)) => println!("outgoing finished with: {:?}", res),
    }

    println!("dropping connection");
    Ok(())
}

async fn incoming_task<S>(
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
                println!("packet: {:?}", packet);
                let event = match packet {
                    Packet::Connect(connect) => Event::Connect(connect, connection.clone()),
                    _ => Event::Unknown,
                };

                broker.send(event).await?;
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

async fn outgoing_task<S>(mut events: Receiver<Event>, mut outgoing: S) -> Result<(), Error>
where
    S: Sink<Packet, Error = EncodeError> + Unpin,
{
    while let Some(event) = events.recv().await {
        match event {
            Event::ConnAck(connack) => outgoing
                .send(Packet::ConnAck(connack))
                .await
                .context(ErrorKind::EncodePacket)?,
            _ => (),
        }
    }
    Ok(())
}
