use crate::error::IggyError;
use async_trait::async_trait;
use bytes::Bytes;

pub mod binary_client;
pub mod consumer_groups;
pub mod consumer_offsets;
mod mapper;
pub mod messages;
pub mod partitions;
pub mod personal_access_tokens;
pub mod streams;
pub mod system;
pub mod topics;
pub mod users;

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

#[async_trait]
pub trait BinaryTransport {
    /// Gets the state of the client.
    async fn get_state(&self) -> ClientState;
    /// Sets the state of the client.
    async fn set_state(&self, state: ClientState);
    /// Sends a command and returns the response.
    async fn send_with_response(&self, command: u32, payload: Bytes) -> Result<Bytes, IggyError>;
}

async fn fail_if_not_authenticated<T: BinaryTransport>(transport: &T) -> Result<(), IggyError> {
    if transport.get_state().await != ClientState::Authenticated {
        return Err(IggyError::Unauthenticated);
    }
    Ok(())
}
