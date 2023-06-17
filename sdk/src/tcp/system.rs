use crate::binary;
use crate::client::SystemClient;
use crate::error::Error;
use crate::system::kill::Kill;
use crate::system::ping::Ping;
use crate::tcp::client::TcpClient;
use async_trait::async_trait;

#[async_trait]
impl SystemClient for TcpClient {
    async fn ping(&self, command: &Ping) -> Result<(), Error> {
        binary::system::ping(self, command).await
    }

    async fn kill(&self, command: &Kill) -> Result<(), Error> {
        binary::system::kill(self, command).await
    }
}
