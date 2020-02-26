use async_trait::async_trait;

use crate::error::Error;
use crate::BrokerState;

#[async_trait]
pub trait Snapshot {
    type Error: Into<Error>;

    async fn load(&mut self) -> Result<BrokerState, Self::Error>;

    async fn store(&mut self, state: BrokerState) -> Result<(), Self::Error>;
}

pub struct NullSnapshot;

#[async_trait]
impl Snapshot for NullSnapshot {
    type Error = Error;

    async fn load(&mut self) -> Result<BrokerState, Self::Error> {
        Ok(BrokerState::default())
    }

    async fn store(&mut self, _state: BrokerState) -> Result<(), Self::Error> {
        Ok(())
    }
}
