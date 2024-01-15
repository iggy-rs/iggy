use crate::client::Client;
use crate::error::Error;
use async_trait::async_trait;

/// The state of the client.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ClientState {
    /// The client is disconnected.
    Disconnected,
    /// The client is connected.
    Connected,
    /// The client is connected and authenticated.
    Authenticated,
}

/// A client that can send and receive binary messages.
#[async_trait]
pub trait BinaryClient: Client {
    /// Gets the state of the client.
    async fn get_state(&self) -> ClientState;
    /// Sets the state of the client.
    async fn set_state(&self, state: ClientState);
    /// Sends a command and returns the response.
    async fn send_with_response(&self, command: u32, payload: &[u8]) -> Result<Vec<u8>, Error>;
}
