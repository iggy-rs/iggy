use crate::client::SystemClient;
use crate::error::Error;
use crate::quic::client::QuicClient;
use async_trait::async_trait;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::system::kill::Kill;
use shared::system::ping::Ping;

#[async_trait]
impl SystemClient for QuicClient {
    async fn ping(&self, command: &Ping) -> Result<(), Error> {
        self.send_with_response(
            [Command::Ping.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }

    async fn kill(&self, command: &Kill) -> Result<(), Error> {
        self.send_with_response(
            [Command::Kill.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }
}
