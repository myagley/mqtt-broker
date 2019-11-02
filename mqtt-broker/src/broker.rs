use std::collections::HashMap;

use failure::ResultExt;
use futures_util::stream::futures_unordered::FuturesUnordered;
use futures_util::stream::StreamExt;
use mqtt::proto;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, info, span, warn, Level};
use tracing_futures::Instrument;

use crate::session::{ConnectedSession, Session, SessionState};
use crate::{ClientId, ConnReq, Error, ErrorKind, Event, Message};

static EXPECTED_PROTOCOL_NAME: &str = "MQTT";
const EXPECTED_PROTOCOL_LEVEL: u8 = 0x4;

macro_rules! try_send {
    ($session:expr, $msg:expr) => {{
        if let Err(e) = $session.send($msg).await {
            warn!(message = "error processing message", %e);
        }
    }};
}

pub struct Broker {
    sender: Sender<Message>,
    messages: Receiver<Message>,
    sessions: HashMap<ClientId, Session>,
}

impl Broker {
    pub fn new() -> Self {
        let (sender, messages) = mpsc::channel(1024);
        Self {
            sender,
            messages,
            sessions: HashMap::new(),
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
        let (client_id, event) = message.into_parts();
        debug!("incoming: {:?}", event);
        let result = match event {
            Event::ConnReq(connreq) => self.process_connect(client_id, connreq).await,
            Event::ConnAck(_) => Ok(info!("broker received CONNACK, ignoring")),
            Event::Disconnect(_) => self.process_disconnect(client_id).await,
            Event::DropConnection => self.process_drop_connection(client_id).await,
            Event::CloseSession => self.process_close_session(client_id).await,
            Event::PingReq(ping) => self.process_ping_req(client_id, ping).await,
            Event::PingResp(_) => Ok(info!("broker received PINGRESP, ignoring")),
            Event::Subscribe(subscribe) => self.process_subscribe(client_id, subscribe).await,
            Event::SubAck(_) => Ok(info!("broker received SUBACK, ignoring")),
            Event::Unsubscribe(unsubscribe) => {
                self.process_unsubscribe(client_id, unsubscribe).await
            }
            Event::UnsubAck(_) => Ok(info!("broker received UNSUBACK, ignoring")),
            Event::PublishFrom(publish) => self.process_publish(client_id, publish).await,
            Event::PublishTo(_publish) => Ok(info!("broker received a PublishTo, ignoring")),
            Event::PubAck0(id) => self.process_puback0(client_id, id).await,
            Event::PubAck(puback) => self.process_puback(client_id, puback).await,
            Event::PubRec(pubrec) => self.process_pubrec(client_id, pubrec).await,
            Event::PubRel(pubrel) => self.process_pubrel(client_id, pubrel).await,
            Event::PubComp(pubcomp) => self.process_pubcomp(client_id, pubcomp).await,
        };

        if let Err(e) = result {
            warn!(message = "error processing message", %e);
        }

        Ok(())
    }

    async fn process_connect(
        &mut self,
        client_id: ClientId,
        mut connreq: ConnReq,
    ) -> Result<(), Error> {
        debug!("handling connect...");

        // [MQTT-3.1.2-1] - If the protocol name is incorrect the Server MAY
        // disconnect the Client, or it MAY continue processing the CONNECT
        // packet in accordance with some other specification.
        // In the latter case, the Server MUST NOT continue to process the
        // CONNECT packet in line with this specification.
        //
        // We will simply disconnect the client and return.
        if &connreq.connect().protocol_name != EXPECTED_PROTOCOL_NAME {
            warn!(
                "invalid protocol name received from client: {}",
                connreq.connect().protocol_name
            );
            debug!("dropping connection due to invalid protocol name");
            let message = Message::new(client_id, Event::DropConnection);
            try_send!(connreq.handle_mut(), message);
            return Ok(());
        }

        // [MQTT-3.1.2-2] - The Server MUST respond to the CONNECT Packet
        // with a CONNACK return code 0x01 (unacceptable protocol level)
        // and then disconnect the Client if the Protocol Level is not supported
        // by the Server.
        if connreq.connect().protocol_level != EXPECTED_PROTOCOL_LEVEL {
            warn!(
                "invalid protocol level received from client: {}",
                connreq.connect().protocol_level
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
            try_send!(connreq.handle_mut(), message);

            debug!("dropping connection due to invalid protocol level");
            let message = Message::new(client_id, Event::DropConnection);
            try_send!(connreq.handle_mut(), message);
            return Ok(());
        }

        // Process the CONNECT packet after it has been validated

        match self.open_session(connreq) {
            Ok(ack) => {
                let should_drop = ack.return_code != proto::ConnectReturnCode::Accepted;

                // Send ConnAck on new session
                let session = self.get_session_mut(&client_id)?;
                session.send(Event::ConnAck(ack)).await?;

                if should_drop {
                    session.send(Event::DropConnection).await?;
                }
            }
            Err(SessionError::DuplicateSession(mut old_session, ack)) => {
                // Drop the old connection
                old_session.send(Event::DropConnection).await?;

                // Send ConnAck on new connection
                let should_drop = ack.return_code != proto::ConnectReturnCode::Accepted;
                let session = self.get_session_mut(&client_id)?;
                session.send(Event::ConnAck(ack)).await?;

                if should_drop {
                    session.send(Event::DropConnection).await?;
                }
            }
            Err(SessionError::ProtocolViolation(mut old_session)) => {
                old_session.send(Event::DropConnection).await?
            }
        }

        debug!("connect handled.");
        Ok(())
    }

    async fn process_disconnect(&mut self, client_id: ClientId) -> Result<(), Error> {
        debug!("handling disconnect...");
        if let Some(mut session) = self.close_session(&client_id) {
            session.send(Event::Disconnect(proto::Disconnect)).await?;
        } else {
            debug!("no session for {}", client_id);
        }
        debug!("disconnect handled.");
        Ok(())
    }

    async fn process_drop_connection(&mut self, client_id: ClientId) -> Result<(), Error> {
        debug!("handling drop connection...");
        if let Some(mut session) = self.close_session(&client_id) {
            session.send(Event::DropConnection).await?;
        } else {
            debug!("no session for {}", client_id);
        }
        debug!("drop connection handled.");
        Ok(())
    }

    async fn process_close_session(&mut self, client_id: ClientId) -> Result<(), Error> {
        debug!("handling close session...");
        if self.close_session(&client_id).is_some() {
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
        match self.get_session_mut(&client_id) {
            Ok(session) => session.send(Event::PingResp(proto::PingResp)).await,
            Err(e) if *e.kind() == ErrorKind::NoSession => {
                debug!("no session for {}", client_id);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn process_subscribe(
        &mut self,
        client_id: ClientId,
        subscribe: proto::Subscribe,
    ) -> Result<(), Error> {
        match self.get_session_mut(&client_id) {
            Ok(session) => {
                // TODO handle retained messages
                let suback = session.subscribe(subscribe)?;
                session.send(Event::SubAck(suback)).await
            }
            Err(e) if *e.kind() == ErrorKind::NoSession => {
                debug!("no session for {}", client_id);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn process_unsubscribe(
        &mut self,
        client_id: ClientId,
        unsubscribe: proto::Unsubscribe,
    ) -> Result<(), Error> {
        match self.get_session_mut(&client_id) {
            Ok(session) => {
                let unsuback = session.unsubscribe(unsubscribe)?;
                session.send(Event::UnsubAck(unsuback)).await
            }
            Err(e) if *e.kind() == ErrorKind::NoSession => {
                debug!("no session for {}", client_id);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn process_publish(
        &mut self,
        client_id: ClientId,
        publish: proto::Publish,
    ) -> Result<(), Error> {
        let maybe_publication = match self.get_session_mut(&client_id) {
            Ok(session) => {
                let (maybe_publication, maybe_event) = session.handle_publish(publish)?;
                if let Some(event) = maybe_event {
                    session.send(event).await?;
                }
                maybe_publication
            }
            Err(e) if *e.kind() == ErrorKind::NoSession => {
                debug!("no session for {}", client_id);
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        if let Some(publication) = maybe_publication {
            self.sessions
                .values_mut()
                .map(|session| session.publish_to(&publication))
                .collect::<FuturesUnordered<_>>()
                .fold(Ok(()), |acc, res| {
                    async move {
                        match (acc, res) {
                            (Ok(()), Err(e)) => Err(e),
                            (a, _) => a,
                        }
                    }
                })
                .await?
        }
        Ok(())
    }

    async fn process_puback(
        &mut self,
        client_id: ClientId,
        puback: proto::PubAck,
    ) -> Result<(), Error> {
        match self.get_session_mut(&client_id) {
            Ok(session) => {
                if let Some(event) = session.handle_puback(&puback)? {
                    session.send(event).await?
                }
                Ok(())
            }
            Err(e) if *e.kind() == ErrorKind::NoSession => {
                debug!("no session for {}", client_id);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn process_puback0(
        &mut self,
        client_id: ClientId,
        id: proto::PacketIdentifier,
    ) -> Result<(), Error> {
        match self.get_session_mut(&client_id) {
            Ok(session) => {
                if let Some(event) = session.handle_puback0(id)? {
                    session.send(event).await?
                }
                Ok(())
            }
            Err(e) if *e.kind() == ErrorKind::NoSession => {
                debug!("no session for {}", client_id);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn process_pubrec(
        &mut self,
        client_id: ClientId,
        pubrec: proto::PubRec,
    ) -> Result<(), Error> {
        match self.get_session_mut(&client_id) {
            Ok(session) => {
                if let Some(event) = session.handle_pubrec(pubrec)? {
                    session.send(event).await?
                }
                Ok(())
            }
            Err(e) if *e.kind() == ErrorKind::NoSession => {
                debug!("no session for {}", client_id);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn process_pubrel(
        &mut self,
        client_id: ClientId,
        pubrel: proto::PubRel,
    ) -> Result<(), Error> {
        let maybe_publication = match self.get_session_mut(&client_id) {
            Ok(session) => {
                let packet_identifier = pubrel.packet_identifier;
                let maybe_publication = session.handle_pubrel(pubrel)?;

                let pubcomp = proto::PubComp { packet_identifier };
                session.send(Event::PubComp(pubcomp)).await?;
                maybe_publication
            }
            Err(e) if *e.kind() == ErrorKind::NoSession => {
                debug!("no session for {}", client_id);
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        if let Some(publication) = maybe_publication {
            self.sessions
                .values_mut()
                .map(|session| session.publish_to(&publication))
                .collect::<FuturesUnordered<_>>()
                .fold(Ok(()), |acc, res| {
                    async move {
                        match (acc, res) {
                            (Ok(()), Err(e)) => Err(e),
                            (a, _) => a,
                        }
                    }
                })
                .await?
        }
        Ok(())
    }

    async fn process_pubcomp(
        &mut self,
        client_id: ClientId,
        pubcomp: proto::PubComp,
    ) -> Result<(), Error> {
        match self.get_session_mut(&client_id) {
            Ok(session) => {
                if let Some(event) = session.handle_pubcomp(pubcomp)? {
                    session.send(event).await?
                }
                Ok(())
            }
            Err(e) if *e.kind() == ErrorKind::NoSession => {
                debug!("no session for {}", client_id);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn get_session_mut(&mut self, client_id: &ClientId) -> Result<&mut Session, Error> {
        self.sessions
            .get_mut(client_id)
            .ok_or(Error::new(ErrorKind::NoSession.into()))
    }

    fn open_session(&mut self, connreq: ConnReq) -> Result<proto::ConnAck, SessionError> {
        let client_id = connreq.client_id().clone();

        match self.sessions.remove(&client_id) {
            Some(Session::Transient(current_connected)) => {
                self.open_session_connected(connreq, current_connected)
            }
            Some(Session::Persistent(current_connected)) => {
                self.open_session_connected(connreq, current_connected)
            }
            Some(Session::Offline(offline)) => {
                debug!("found an offline session for {}", client_id);

                let (new_session, session_present) = if let proto::ClientId::IdWithExistingSession(
                    _,
                ) = connreq.connect().client_id
                {
                    debug!("moving offline session to online for {}", client_id);
                    let new_session = Session::new_persistent(connreq, offline.into_state());
                    (new_session, true)
                } else {
                    info!("cleaning offline session for {}", client_id);
                    let new_session = Session::new_transient(connreq);
                    (new_session, false)
                };

                self.sessions.insert(client_id, new_session);

                let ack = proto::ConnAck {
                    session_present,
                    return_code: proto::ConnectReturnCode::Accepted,
                };

                Ok(ack)
            }
            Some(Session::Disconnecting(client_id, handle)) => Err(
                SessionError::ProtocolViolation(Session::Disconnecting(client_id, handle)),
            ),
            None => {
                // No session present - create a new one.
                let new_session = if let proto::ClientId::IdWithExistingSession(_) =
                    connreq.connect().client_id
                {
                    debug!("creating new persistent session for {}", client_id);
                    let state = SessionState::new(client_id.clone(), &connreq);
                    Session::new_persistent(connreq, state)
                } else {
                    debug!("creating new transient session for {}", client_id);
                    Session::new_transient(connreq)
                };

                self.sessions.insert(client_id.clone(), new_session);

                let ack = proto::ConnAck {
                    session_present: false,
                    return_code: proto::ConnectReturnCode::Accepted,
                };

                Ok(ack)
            }
        }
    }

    fn open_session_connected(
        &mut self,
        connreq: ConnReq,
        current_connected: ConnectedSession,
    ) -> Result<proto::ConnAck, SessionError> {
        if current_connected.handle() == connreq.handle() {
            // [MQTT-3.1.0-2] - The Server MUST process a second CONNECT Packet
            // sent from a Client as a protocol violation and disconnect the Client.
            //
            // If the handles are equal, this is a second CONNECT packet on the
            // same physical connection. This condition is handled by the Connection
            // handling code. If this state gets to the broker, something is seriously broken
            // and we should just abort.

            panic!("Second CONNECT on same connection reached the broker. Connection handling logic should prevent this.");
        } else {
            // [MQTT-3.1.4-2] If the ClientId represents a Client already connected to the Server
            // then the Server MUST disconnect the existing Client.
            //
            // Send a DropConnection to the current handle.
            // Update the session to use the new handle.

            info!(
                "connection request for an in use client id ({}). closing previous connection",
                connreq.client_id()
            );

            let client_id = connreq.client_id().clone();
            let (state, handle) = current_connected.into_parts();
            let (new_session, old_session, session_present) =
                if let proto::ClientId::IdWithExistingSession(_) = connreq.connect().client_id {
                    debug!(
                        "moving persistent session to this connection for {}",
                        client_id
                    );
                    let old_session = Session::Disconnecting(connreq.client_id().clone(), handle);
                    let new_session = Session::new_persistent(connreq, state);
                    (new_session, old_session, true)
                } else {
                    info!("cleaning session for {}", client_id);
                    let old_session = Session::Disconnecting(connreq.client_id().clone(), handle);
                    let new_session = Session::new_transient(connreq);
                    (new_session, old_session, false)
                };

            self.sessions.insert(client_id, new_session);
            let ack = proto::ConnAck {
                session_present,
                return_code: proto::ConnectReturnCode::Accepted,
            };

            Err(SessionError::DuplicateSession(old_session, ack))
        }
    }

    fn close_session(&mut self, client_id: &ClientId) -> Option<Session> {
        match self.sessions.remove(client_id) {
            Some(Session::Transient(connected)) => {
                debug!("closing transient session for {}", client_id);
                Some(Session::Disconnecting(
                    client_id.clone(),
                    connected.into_handle(),
                ))
            }
            Some(Session::Persistent(connected)) => {
                // Move a persistent session into the offline state
                // Return a disconnecting session to allow a disconnect
                // to be sent on the connection

                debug!("moving persistent session to offline for {}", client_id);
                let (state, handle) = connected.into_parts();
                let new_session = Session::new_offline(state);
                self.sessions.insert(client_id.clone(), new_session);
                Some(Session::Disconnecting(client_id.clone(), handle))
            }
            Some(Session::Offline(offline)) => {
                debug!("closing already offline session for {}", client_id);
                self.sessions
                    .insert(client_id.clone(), Session::Offline(offline));
                None
            }
            _ => None,
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

#[derive(Debug)]
pub enum SessionError {
    ProtocolViolation(Session),
    DuplicateSession(Session, proto::ConnAck),
}

#[cfg(test)]
mod tests {
    use super::*;

    use matches::assert_matches;
    use uuid::Uuid;

    use crate::ConnectionHandle;

    fn connection_handle() -> ConnectionHandle {
        let id = Uuid::new_v4();
        let (tx1, _rx1) = mpsc::channel(128);
        ConnectionHandle::new(id, tx1)
    }

    fn transient_connect(id: String) -> proto::Connect {
        proto::Connect {
            username: None,
            password: None,
            will: None,
            client_id: proto::ClientId::IdWithCleanSession(id),
            keep_alive: Default::default(),
            protocol_name: "MQTT".to_string(),
            protocol_level: 0x4,
        }
    }

    fn persistent_connect(id: String) -> proto::Connect {
        proto::Connect {
            username: None,
            password: None,
            will: None,
            client_id: proto::ClientId::IdWithExistingSession(id),
            keep_alive: Default::default(),
            protocol_name: "MQTT".to_string(),
            protocol_level: 0x4,
        }
    }

    #[tokio::test]
    #[should_panic]
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
        let conn1 = ConnectionHandle::new(id, tx1.clone());
        let conn2 = ConnectionHandle::new(id, tx1);
        let client_id = ClientId::from("blah".to_string());

        let req1 = ConnReq::new(client_id.clone(), connect1, conn1);
        let req2 = ConnReq::new(client_id.clone(), connect2, conn2);

        broker_handle
            .send(Message::new(client_id.clone(), Event::ConnReq(req1)))
            .await
            .unwrap();
        broker_handle
            .send(Message::new(client_id.clone(), Event::ConnReq(req2)))
            .await
            .unwrap();

        assert_matches!(rx1.recv().await.unwrap().event(), Event::ConnAck(_));
        assert_matches!(rx1.recv().await.unwrap().event(), Event::DropConnection);
        assert!(rx1.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_double_connect_drop_first_transient() {
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

        let req1 = ConnReq::new(client_id.clone(), connect1, conn1);
        let req2 = ConnReq::new(client_id.clone(), connect2, conn2);

        broker_handle
            .send(Message::new(client_id.clone(), Event::ConnReq(req1)))
            .await
            .unwrap();
        broker_handle
            .send(Message::new(client_id.clone(), Event::ConnReq(req2)))
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
        let req1 = ConnReq::new(client_id.clone(), connect1, conn1);

        broker_handle
            .send(Message::new(client_id.clone(), Event::ConnReq(req1)))
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
        let req1 = ConnReq::new(client_id.clone(), connect1, conn1);

        broker_handle
            .send(Message::new(client_id.clone(), Event::ConnReq(req1)))
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

    #[test]
    fn test_add_session_empty_transient() {
        let id = "id1".to_string();
        let mut broker = Broker::default();
        let client_id = ClientId::from(id.clone());
        let connect = transient_connect(id.clone());
        let handle = connection_handle();
        let req = ConnReq::new(client_id.clone(), connect, handle);

        broker.open_session(req).unwrap();

        // check new session
        assert_eq!(1, broker.sessions.len());
        assert_matches!(broker.sessions[&client_id], Session::Transient(_));

        // close session and check behavior
        let old_session = broker.close_session(&client_id);
        assert_matches!(old_session, Some(Session::Disconnecting(_, _)));
        assert_eq!(0, broker.sessions.len());
    }

    #[test]
    fn test_add_session_empty_persistent() {
        let id = "id1".to_string();
        let mut broker = Broker::default();
        let client_id = ClientId::from(id.clone());
        let connect = persistent_connect(id.clone());
        let handle = connection_handle();
        let req = ConnReq::new(client_id.clone(), connect, handle);

        broker.open_session(req).unwrap();

        assert_eq!(1, broker.sessions.len());
        assert_matches!(broker.sessions[&client_id], Session::Persistent(_));

        // close session and check behavior
        let old_session = broker.close_session(&client_id);
        assert_matches!(old_session, Some(Session::Disconnecting(_, _)));
        assert_eq!(1, broker.sessions.len());
        assert_matches!(broker.sessions[&client_id], Session::Offline(_));
    }

    #[test]
    #[should_panic]
    fn test_add_session_same_connection_transient() {
        let id = "id1".to_string();
        let mut broker = Broker::default();
        let client_id = ClientId::from(id.clone());
        let connect1 = transient_connect(id.clone());
        let connect2 = transient_connect(id.clone());
        let id = Uuid::new_v4();
        let (tx1, _rx1) = mpsc::channel(128);
        let handle1 = ConnectionHandle::new(id.clone(), tx1.clone());
        let handle2 = ConnectionHandle::new(id, tx1);

        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);
        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);

        broker.open_session(req1).unwrap();
        assert_eq!(1, broker.sessions.len());

        let result = broker.open_session(req2);
        assert_matches!(result, Err(SessionError::ProtocolViolation(_)));
        assert_eq!(0, broker.sessions.len());
    }

    #[test]
    #[should_panic]
    fn test_add_session_same_connection_persistent() {
        let id = "id1".to_string();
        let mut broker = Broker::default();
        let client_id = ClientId::from(id.clone());
        let connect1 = persistent_connect(id.clone());
        let connect2 = persistent_connect(id.clone());
        let id = Uuid::new_v4();
        let (tx1, _rx1) = mpsc::channel(128);
        let handle1 = ConnectionHandle::new(id.clone(), tx1.clone());
        let handle2 = ConnectionHandle::new(id, tx1);

        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);
        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);

        broker.open_session(req1).unwrap();
        assert_eq!(1, broker.sessions.len());

        let result = broker.open_session(req2);
        assert_matches!(result, Err(SessionError::ProtocolViolation(_)));
        assert_eq!(0, broker.sessions.len());
    }

    #[test]
    fn test_add_session_different_connection_transient_then_transient() {
        let id = "id1".to_string();
        let mut broker = Broker::default();
        let client_id = ClientId::from(id.clone());
        let connect1 = transient_connect(id.clone());
        let connect2 = transient_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);

        broker.open_session(req1).unwrap();
        assert_eq!(1, broker.sessions.len());

        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);
        let result = broker.open_session(req2);
        assert_matches!(result, Err(SessionError::DuplicateSession(_, _)));
        assert_matches!(broker.sessions[&client_id], Session::Transient(_));
        assert_eq!(1, broker.sessions.len());
    }

    #[test]
    fn test_add_session_different_connection_transient_then_persistent() {
        let id = "id1".to_string();
        let mut broker = Broker::default();
        let client_id = ClientId::from(id.clone());
        let connect1 = transient_connect(id.clone());
        let connect2 = persistent_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);

        broker.open_session(req1).unwrap();
        assert_eq!(1, broker.sessions.len());

        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);
        let result = broker.open_session(req2);
        assert_matches!(result, Err(SessionError::DuplicateSession(_, _)));
        assert_matches!(broker.sessions[&client_id], Session::Persistent(_));
        assert_eq!(1, broker.sessions.len());
    }

    #[test]
    fn test_add_session_different_connection_persistent_then_transient() {
        let id = "id1".to_string();
        let mut broker = Broker::default();
        let client_id = ClientId::from(id.clone());
        let connect1 = persistent_connect(id.clone());
        let connect2 = transient_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);

        broker.open_session(req1).unwrap();
        assert_eq!(1, broker.sessions.len());

        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);
        let result = broker.open_session(req2);
        assert_matches!(result, Err(SessionError::DuplicateSession(_, _)));
        assert_matches!(broker.sessions[&client_id], Session::Transient(_));
        assert_eq!(1, broker.sessions.len());
    }

    #[test]
    fn test_add_session_different_connection_persistent_then_persistent() {
        let id = "id1".to_string();
        let mut broker = Broker::default();
        let client_id = ClientId::from(id.clone());
        let connect1 = persistent_connect(id.clone());
        let connect2 = persistent_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);
        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);

        broker.open_session(req1).unwrap();
        assert_eq!(1, broker.sessions.len());

        let result = broker.open_session(req2);
        assert_matches!(result, Err(SessionError::DuplicateSession(_, _)));
        assert_matches!(broker.sessions[&client_id], Session::Persistent(_));
        assert_eq!(1, broker.sessions.len());
    }

    #[test]
    fn test_add_session_offline_persistent() {
        let id = "id1".to_string();
        let mut broker = Broker::default();
        let client_id = ClientId::from(id.clone());
        let connect1 = persistent_connect(id.clone());
        let connect2 = persistent_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);
        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);

        broker.open_session(req1).unwrap();

        assert_eq!(1, broker.sessions.len());
        assert_matches!(broker.sessions[&client_id], Session::Persistent(_));

        // close session and check behavior
        let old_session = broker.close_session(&client_id);
        assert_matches!(old_session, Some(Session::Disconnecting(_, _)));
        assert_eq!(1, broker.sessions.len());
        assert_matches!(broker.sessions[&client_id], Session::Offline(_));

        // Reopen session
        broker.open_session(req2).unwrap();

        assert_eq!(1, broker.sessions.len());
        assert_matches!(broker.sessions[&client_id], Session::Persistent(_));
    }

    #[test]
    fn test_add_session_offline_transient() {
        let id = "id1".to_string();
        let mut broker = Broker::default();
        let client_id = ClientId::from(id.clone());
        let connect1 = persistent_connect(id.clone());
        let handle1 = connection_handle();
        let handle2 = connection_handle();
        let req1 = ConnReq::new(client_id.clone(), connect1, handle1);

        broker.open_session(req1).unwrap();

        assert_eq!(1, broker.sessions.len());
        assert_matches!(broker.sessions[&client_id], Session::Persistent(_));

        // close session and check behavior
        let old_session = broker.close_session(&client_id);
        assert_matches!(old_session, Some(Session::Disconnecting(_, _)));
        assert_eq!(1, broker.sessions.len());
        assert_matches!(broker.sessions[&client_id], Session::Offline(_));

        // Reopen session
        let connect2 = transient_connect(id.clone());
        let req2 = ConnReq::new(client_id.clone(), connect2, handle2);
        broker.open_session(req2).unwrap();

        assert_eq!(1, broker.sessions.len());
        assert_matches!(broker.sessions[&client_id], Session::Transient(_));
    }
}
