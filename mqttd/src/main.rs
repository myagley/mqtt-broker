use std::{env, io};

use futures_util::pin_mut;
use mqtt_broker::{Error, Server};
use tracing::Level;
use tracing_subscriber::{fmt, EnvFilter};

mod shutdown;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let subscriber = fmt::Subscriber::builder()
        .with_ansi(atty::is(atty::Stream::Stderr))
        .with_max_level(Level::TRACE)
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let addr = env::args().nth(1).unwrap_or("0.0.0.0:1883".to_string());

    let shutdown = shutdown::shutdown();
    pin_mut!(shutdown);

    let _state = Server::new().serve(addr, shutdown).await?;

    // TODO - sync state to disk
    Ok(())
}
