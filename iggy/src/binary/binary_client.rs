use crate::client::Client;
use crate::error::Error;
use async_trait::async_trait;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ClientState {
    Disconnected,
    Connected,
    Authenticated,
}

#[async_trait]
pub trait BinaryClient: Client {
    async fn get_state(&self) -> ClientState;
    async fn set_state(&self, state: ClientState);
    async fn send_with_response(&self, command: u32, payload: &[u8]) -> Result<Vec<u8>, Error>;
}
