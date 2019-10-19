use failure::ResultExt;
use mqtt::proto::*;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{Error, ErrorKind, Event};

pub struct Broker {
    sender: Sender<Event>,
    events: Receiver<Event>,
}

impl Broker {
    pub fn new() -> Self {
        let (sender, events) = mpsc::channel(1024);
        Self { sender, events }
    }

    pub fn handle(&self) -> BrokerHandle {
        BrokerHandle(self.sender.clone())
    }

    pub async fn run(mut self) {
        while let Some(event) = self.events.recv().await {
            match event {
                Event::Connect(_conn, mut handle) => {
                    let ack = ConnAck {
                        session_present: false,
                        return_code: ConnectReturnCode::Accepted,
                    };
                    if let Err(e) = handle.send(Event::ConnAck(ack)).await {
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
pub struct BrokerHandle(Sender<Event>);

impl BrokerHandle {
    pub async fn send(&mut self, event: Event) -> Result<(), Error> {
        self.0
            .send(event)
            .await
            .context(ErrorKind::SendBrokerEvent)?;
        Ok(())
    }
}
