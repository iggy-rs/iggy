use async_trait::async_trait;
use sdk::error::Error;

#[async_trait]
pub trait Sender: Sync + Send {
    async fn send_empty_ok_response(&mut self) -> Result<(), Error>;
    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), Error>;
    async fn send_error_response(&mut self, error: Error) -> Result<(), Error>;
}
