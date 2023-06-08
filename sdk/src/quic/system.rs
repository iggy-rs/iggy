use crate::binary;
use crate::client::SystemClient;
use crate::error::Error;
use crate::quic::client::QuicClient;
use async_trait::async_trait;
use shared::system::kill::Kill;
use shared::system::ping::Ping;

#[async_trait]
impl SystemClient for QuicClient {
    async fn ping(&self, command: Ping) -> Result<(), Error> {
        binary::system::ping(self, command).await
    }

    async fn kill(&self, command: Kill) -> Result<(), Error> {
        binary::system::kill(self, command).await
    }
}
