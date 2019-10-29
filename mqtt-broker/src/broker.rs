use failure::ResultExt;
use mqtt::proto;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, info, span, warn, Level};
use tracing_futures::Instrument;

use crate::session::{SessionError, SessionManager};
use crate::{ClientId, ConnectionHandle, Error, ErrorKind, Event, Message};

static EXPECTED_PROTOCOL_NAME: &str = "MQTT";
const EXPECTED_PROTOCOL_LEVEL: u8 = 0x4;

macro_rules! try_send {
    ($session:ident, $msg:expr) => {{
        if let Err(e) = $session.send($msg).await {
            warn!(message = "error processing message", %e);
        }
    }};
}

pub struct Broker {
    sender: Sender<Message>,
    messages: Receiver<Message>,
    sessions: SessionManager,
}

impl Broker {
    pub fn new() -> Self {
        let (sender, messages) = mpsc::channel(1024);
        Self {
            sender,
            messages,
            sessions: SessionManager::new(),
        }
    }

    pub fn handle(&self) -> BrokerHandle {
        BrokerHandle(self.sender.clone())
    }

    pub async fn run(mut self) {
        while let Some(message) = self.messages.recv().await {
            let span = span!(Level::INFO, "broker", client_id=%message.client_id());
            if let Err(e) = self.process_message(message).instrument(span).await {
                warn!(message = "an error occurred processing a message", error=%e);
            }
        }
        info!("broker task exiting");
    }

    async fn process_message(&mut self, message: Message) -> Result<(), Error> {
        let client_id = message.client_id().clone();
        let result = match message.into_event() {
            Event::Connect(connect, handle) => {
                self.process_connect(client_id, connect, handle).await
            }
            Event::ConnAck(_) => Ok(debug!("broker received CONNACK, ignoring")),
            Event::Disconnect(_) => self.process_disconnect(client_id).await,
            Event::DropConnection => self.process_drop_connection(client_id).await,
            Event::CloseSession => self.process_close_session(client_id).await,
            Event::PingReq(ping) => self.process_ping_req(client_id, ping).await,
            Event::PingResp(_) => Ok(debug!("broker received PINGRESP, ignoring")),
            Event::Unknown => Ok(debug!("broker received unknown event, ignoring")),
        };

        if let Err(e) = result {
            warn!(message = "error processing message", %e);
        }

        Ok(())
    }

    async fn process_connect(
        &mut self,
        client_id: ClientId,
        connect: proto::Connect,
        mut handle: ConnectionHandle,
    ) -> Result<(), Error> {
        debug!("handling connect...");

        // [MQTT-3.1.2-1] - If the protocol name is incorrect the Server MAY
        // disconnect the Client, or it MAY continue processing the CONNECT
        // packet in accordance with some other specification.
        // In the latter case, the Server MUST NOT continue to process the
        // CONNECT packet in line with this specification.
        //
        // We will simply disconnect the client and return.
        if &connect.protocol_name != EXPECTED_PROTOCOL_NAME {
            warn!(
                "invalid protocol name received from client: {}",
                connect.protocol_name
            );
            debug!("dropping connection due to invalid protocol name");
            let message = Message::new(client_id.clone(), Event::DropConnection);
            try_send!(handle, message);
            return Ok(());
        }

        // [MQTT-3.1.2-2] - The Server MUST respond to the CONNECT Packet
        // with a CONNACK return code 0x01 (unacceptable protocol level)
        // and then disconnect the Client if the Protocol Level is not supported
        // by the Server.
        if connect.protocol_level != EXPECTED_PROTOCOL_LEVEL {
            warn!(
                "invalid protocol level received from client: {}",
                connect.protocol_level
            );
            let ack = proto::ConnAck {
                session_present: false,
                return_code: proto::ConnectReturnCode::Refused(
                    proto::ConnectionRefusedReason::UnacceptableProtocolVersion,
                ),
            };

            debug!("sending connack...");
            let event = Event::ConnAck(ack);
            let message = Message::new(client_id.clone(), event);
            try_send!(handle, message);

            debug!("dropping connection due to invalid protocol level");
            let message = Message::new(client_id.clone(), Event::DropConnection);
            try_send!(handle, message);
            return Ok(());
        }

        // Process the CONNECT packet after it has been validated

        match self
            .sessions
            .open_session(client_id.clone(), &connect, handle)
        {
            Ok(ack) => {
                let should_drop = ack.return_code != proto::ConnectReturnCode::Accepted;

                // Send ConnAck on new session
                self.sessions.send(&client_id, Event::ConnAck(ack)).await?;

                if should_drop {
                    self.sessions
                        .send(&client_id, Event::DropConnection)
                        .await?;
                }
            }
            Err(SessionError::DuplicateSession(mut session, ack)) => {
                // Drop the old connection
                session.send(Event::DropConnection).await?;

                // Send ConnAck on new connection
                let should_drop = ack.return_code != proto::ConnectReturnCode::Accepted;
                self.sessions.send(&client_id, Event::ConnAck(ack)).await?;

                if should_drop {
                    self.sessions
                        .send(&client_id, Event::DropConnection)
                        .await?;
                }
            }
            Err(SessionError::ProtocolViolation(mut session)) => {
                session.send(Event::DropConnection).await?
            }
        }

        debug!("connect handled.");
        Ok(())
    }

    async fn process_disconnect(&mut self, client_id: ClientId) -> Result<(), Error> {
        debug!("handling disconnect...");
        if let Some(mut session) = self.sessions.close_session(&client_id) {
            session.send(Event::Disconnect(proto::Disconnect)).await?;
        } else {
            debug!("no session for {}", client_id);
        }
        debug!("disconnect handled.");
        Ok(())
    }

    async fn process_drop_connection(&mut self, client_id: ClientId) -> Result<(), Error> {
        debug!("handling drop connection...");
        if let Some(mut session) = self.sessions.close_session(&client_id) {
            session.send(Event::DropConnection).await?;
        } else {
            debug!("no session for {}", client_id);
        }
        debug!("drop connection handled.");
        Ok(())
    }

    async fn process_close_session(&mut self, client_id: ClientId) -> Result<(), Error> {
        debug!("handling close session...");
        if self.sessions.close_session(&client_id).is_some() {
            debug!("session removed");
        } else {
            debug!("no session for {}", client_id);
        }
        debug!("close session handled.");
        Ok(())
    }

    async fn process_ping_req(
        &mut self,
        client_id: ClientId,
        _ping: proto::PingReq,
    ) -> Result<(), Error> {
        debug!("handling ping request...");

        match self
            .sessions
            .send(&client_id, Event::PingResp(proto::PingResp))
            .await
        {
            Ok(_) => debug!("ping response handled."),
            Err(e) if *e.kind() == ErrorKind::NoSession => debug!("no session for {}", client_id),
            Err(e) => return Err(e),
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

#[cfg(test)]
mod tests {
    use super::*;

    use matches::assert_matches;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_double_connect_protocol_violation() {
        let broker = Broker::default();
        let mut broker_handle = broker.handle();
        tokio::spawn(broker.run());

        let connect1 = proto::Connect {
            username: None,
            password: None,
            will: None,
            client_id: proto::ClientId::IdWithCleanSession("blah".to_string()),
            keep_alive: Default::default(),
            protocol_name: "MQTT".to_string(),
            protocol_level: 0x4,
        };
        let connect2 = proto::Connect {
            username: None,
            password: None,
            will: None,
            client_id: proto::ClientId::IdWithCleanSession("blah".to_string()),
            keep_alive: Default::default(),
            protocol_name: "MQTT".to_string(),
            protocol_level: 0x4,
        };
        let id = Uuid::new_v4();
        let (tx1, mut rx1) = mpsc::channel(128);
        let conn1 = ConnectionHandle::new(id, tx1);
        let conn2 = conn1.clone();
        let client_id = ClientId::from("blah".to_string());

        broker_handle
            .send(Message::new(
                client_id.clone(),
                Event::Connect(connect1, conn1),
            ))
            .await
            .unwrap();
        broker_handle
            .send(Message::new(
                client_id.clone(),
                Event::Connect(connect2, conn2),
            ))
            .await
            .unwrap();

        assert_matches!(rx1.recv().await.unwrap().event(), Event::ConnAck(_));
        assert_matches!(rx1.recv().await.unwrap().event(), Event::DropConnection);
        assert!(rx1.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_double_connect_drop_first() {
        let broker = Broker::default();
        let mut broker_handle = broker.handle();
        tokio::spawn(broker.run());

        let connect1 = proto::Connect {
            username: None,
            password: None,
            will: None,
            client_id: proto::ClientId::IdWithCleanSession("blah".to_string()),
            keep_alive: Default::default(),
            protocol_name: "MQTT".to_string(),
            protocol_level: 0x4,
        };
        let connect2 = proto::Connect {
            username: None,
            password: None,
            will: None,
            client_id: proto::ClientId::IdWithCleanSession("blah".to_string()),
            keep_alive: Default::default(),
            protocol_name: "MQTT".to_string(),
            protocol_level: 0x4,
        };
        let (tx1, mut rx1) = mpsc::channel(128);
        let (tx2, mut rx2) = mpsc::channel(128);
        let conn1 = ConnectionHandle::from_sender(tx1);
        let conn2 = ConnectionHandle::from_sender(tx2);
        let client_id = ClientId::from("blah".to_string());

        broker_handle
            .send(Message::new(
                client_id.clone(),
                Event::Connect(connect1, conn1),
            ))
            .await
            .unwrap();
        broker_handle
            .send(Message::new(
                client_id.clone(),
                Event::Connect(connect2, conn2),
            ))
            .await
            .unwrap();

        assert_matches!(rx1.recv().await.unwrap().event(), Event::ConnAck(_));
        assert_matches!(rx1.recv().await.unwrap().event(), Event::DropConnection);
        assert!(rx1.recv().await.is_none());

        assert_matches!(rx2.recv().await.unwrap().event(), Event::ConnAck(_));
    }

    #[tokio::test]
    async fn test_invalid_protocol_name() {
        let broker = Broker::default();
        let mut broker_handle = broker.handle();
        tokio::spawn(broker.run());

        let connect1 = proto::Connect {
            username: None,
            password: None,
            will: None,
            client_id: proto::ClientId::IdWithCleanSession("blah".to_string()),
            keep_alive: Default::default(),
            protocol_name: "AMQP".to_string(),
            protocol_level: 0x4,
        };
        let (tx1, mut rx1) = mpsc::channel(128);
        let conn1 = ConnectionHandle::from_sender(tx1);
        let client_id = ClientId::from("blah".to_string());

        broker_handle
            .send(Message::new(
                client_id.clone(),
                Event::Connect(connect1, conn1),
            ))
            .await
            .unwrap();

        assert_matches!(rx1.recv().await.unwrap().event(), Event::DropConnection);
        assert!(rx1.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_invalid_protocol_level() {
        let broker = Broker::default();
        let mut broker_handle = broker.handle();
        tokio::spawn(broker.run());

        let connect1 = proto::Connect {
            username: None,
            password: None,
            will: None,
            client_id: proto::ClientId::IdWithCleanSession("blah".to_string()),
            keep_alive: Default::default(),
            protocol_name: "MQTT".to_string(),
            protocol_level: 0x3,
        };
        let (tx1, mut rx1) = mpsc::channel(128);
        let conn1 = ConnectionHandle::from_sender(tx1);
        let client_id = ClientId::from("blah".to_string());

        broker_handle
            .send(Message::new(
                client_id.clone(),
                Event::Connect(connect1, conn1),
            ))
            .await
            .unwrap();

        assert_matches!(
            rx1.recv().await.unwrap().event(),
            Event::ConnAck(proto::ConnAck {
                return_code:
                    proto::ConnectReturnCode::Refused(
                        proto::ConnectionRefusedReason::UnacceptableProtocolVersion,
                    ),
                ..
            })
        );
        assert_matches!(rx1.recv().await.unwrap().event(), Event::DropConnection);
        assert!(rx1.recv().await.is_none());
    }
}
