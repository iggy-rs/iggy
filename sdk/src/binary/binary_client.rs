use crate::client::Client;
use crate::error::Error;
use async_trait::async_trait;

#[async_trait]
pub trait BinaryClient: Client {
    async fn send_with_response(&self, buffer: &[u8]) -> Result<Vec<u8>, Error>;
}
