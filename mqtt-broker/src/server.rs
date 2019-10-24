use std::fmt::Display;

use failure::ResultExt;
use futures_util::stream::StreamExt;
use futures_util::FutureExt;
use tokio::net::TcpListener;
use tokio_net::ToSocketAddrs;

use crate::broker::Broker;
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

        let mut incoming = TcpListener::bind(&addr)
            .await
            .context(ErrorKind::BindServer)?
            .incoming();
        println!("Listening on port: {}", addr);

        // TODO: handle the broker returning an error.
        // TODO: handle server graceful shutdown
        tokio::spawn(broker.run().map(drop));

        while let Some(Ok(stream)) = incoming.next().await {
            let broker_handle = handle.clone();
            tokio::spawn(async move {
                if let Err(e) = connection::process(stream, broker_handle).await {
                    println!("failed to process connection; error = {}", e);
                }
            });
        }
        Ok(())
    }
}
