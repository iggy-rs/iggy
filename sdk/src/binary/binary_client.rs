use crate::client::Client;
use crate::error::Error;
use async_trait::async_trait;
use shared::command::Command;

#[async_trait]
pub trait BinaryClient: Client {
    async fn send_with_response(&self, command: Command) -> Result<Vec<u8>, Error>;
}
