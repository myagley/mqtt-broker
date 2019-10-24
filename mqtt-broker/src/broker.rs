use failure::ResultExt;
use mqtt::proto::*;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, info, span, warn, Level};
use tracing_futures::Instrument;

use crate::{Error, ErrorKind, Event, Message};

pub struct Broker {
    sender: Sender<Message>,
    messages: Receiver<Message>,
}

impl Broker {
    pub fn new() -> Self {
        let (sender, messages) = mpsc::channel(1024);
        Self { sender, messages }
    }

    pub fn handle(&self) -> BrokerHandle {
        BrokerHandle(self.sender.clone())
    }

    pub async fn run(mut self) -> Result<(), Error> {
        while let Some(message) = self.messages.recv().await {
            let span = span!(Level::INFO, "broker", client_id=%message.client_id());
            self.handle_message(message).instrument(span).await?
        }
        info!("broker task exiting");
        Ok(())
    }

    async fn handle_message(&mut self, message: Message) -> Result<(), Error> {
        let client_id = message.client_id().clone();
        match message.into_event() {
            Event::Connect(_conn, mut handle) => {
                debug!("received connect");
                let ack = ConnAck {
                    session_present: false,
                    return_code: ConnectReturnCode::Accepted,
                };
                let event = Event::ConnAck(ack);
                debug!("sending connack");
                if let Err(e) = handle.send(Message::new(client_id, event)).await {
                    warn!(message = "error sending message to connection handle", %e);
                }
            }
            _ => (),
        }
        Ok(())
    }
}

impl Default for Broker {
    fn default() -> Self {
        Broker::new()
    }
}

#[derive(Clone, Debug)]
pub struct BrokerHandle(Sender<Message>);

impl BrokerHandle {
    pub async fn send(&mut self, message: Message) -> Result<(), Error> {
        self.0
            .send(message)
            .await
            .context(ErrorKind::SendBrokerMessage)?;
        Ok(())
    }
}
