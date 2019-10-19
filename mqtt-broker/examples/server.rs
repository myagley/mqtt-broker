use std::env;

use mqtt_broker::{Error, Server};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let addr = env::args().nth(1).unwrap_or("127.0.0.1:1883".to_string());

    Server::new().serve(addr).await
}
