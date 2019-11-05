use std::fmt::Display;
use std::future::Future;

use failure::ResultExt;

use futures_util::future;
use futures_util::stream::StreamExt;
use tokio::net::TcpListener;
use tokio_net::ToSocketAddrs;
use tracing::{info, span, warn, Level};
use tracing_futures::Instrument;

use crate::broker::{Broker, BrokerHandle};
use crate::{connection, Error, ErrorKind};

pub struct Server {
    broker: Broker,
}

impl Server {
    pub fn new() -> Self {
        Self {
            broker: Default::default(),
        }
    }

    pub async fn serve<A>(self, addr: A) -> Result<(), Error>
    where
        A: ToSocketAddrs + Display,
    {
        let Server { broker } = self;
        let handle = broker.handle();

        let broker_task = broker.run();
        let incoming_task = incoming_task(addr, handle.clone());

        match future::join(broker_task, incoming_task).await {
            (_, res) => res?,
        }
        Ok(())
    }
}

async fn incoming_task<A>(addr: A, handle: BrokerHandle) -> Result<(), Error>
where
    A: ToSocketAddrs + Display,
{
    let span = span!(Level::INFO, "server", listener=%addr);
    let _enter = span.enter();

    let mut incoming = TcpListener::bind(&addr)
        .await
        .context(ErrorKind::BindServer)?
        .incoming();
    info!("Listening on address {}", addr);

    while let Some(Ok(stream)) = incoming.next().await {
        let broker_handle = handle.clone();
        let span = span.clone();
        tokio::spawn(async move {
            if let Err(e) = connection::process(stream, broker_handle)
                .instrument(span)
                    .await
            {
                warn!(message = "failed to process connection", error=%e);
            }
        });
    }
    Ok(())
}
