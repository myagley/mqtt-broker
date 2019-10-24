use failure::ResultExt;
use mqtt::proto::*;
use tokio::sync::mpsc::{self, Receiver, Sender};

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

    pub async fn run(mut self) {
        while let Some(message) = self.messages.recv().await {
            let client_id = message.client_id().clone();
            match message.into_event() {
                Event::Connect(_conn, mut handle) => {
                    let ack = ConnAck {
                        session_present: false,
                        return_code: ConnectReturnCode::Accepted,
                    };
                    let event = Event::ConnAck(ack);
                    if let Err(e) = handle.send(Message::new(client_id, event)).await {
                        println!("error sending to connection handle: {}", e);
                    }
                }
                _ => (),
            }
        }
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
