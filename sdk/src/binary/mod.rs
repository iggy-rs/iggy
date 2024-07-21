use crate::command::Command;
use crate::error::IggyError;
use async_trait::async_trait;
use bytes::Bytes;
use derive_more::Display;

#[allow(deprecated)]
pub mod binary_client;
#[allow(deprecated)]
pub mod consumer_groups;
#[allow(deprecated)]
pub mod consumer_offsets;
mod mapper;
#[allow(deprecated)]
pub mod messages;
#[allow(deprecated)]
pub mod partitions;
#[allow(deprecated)]
pub mod personal_access_tokens;
#[allow(deprecated)]
pub mod streams;
#[allow(deprecated)]
pub mod system;
#[allow(deprecated)]
pub mod topics;
#[allow(deprecated)]
pub mod users;

/// The state of the client.
#[derive(Debug, Copy, Clone, PartialEq, Display)]
pub enum ClientState {
    /// The client is disconnected.
    #[display(fmt = "disconnected")]
    Disconnected,
    /// The client is connected.
    #[display(fmt = "connected")]
    Connected,
    /// The client is connected and authenticated.
    #[display(fmt = "authenticated")]
    Authenticated,
}

#[async_trait]
pub trait BinaryTransport {
    /// Gets the state of the client.
    async fn get_state(&self) -> ClientState;
    /// Sets the state of the client.
    async fn set_state(&self, state: ClientState);
    /// Sends a command and returns the response.
    async fn send_with_response<T: Command>(&self, command: &T) -> Result<Bytes, IggyError>;
    async fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError>;
}

async fn fail_if_not_authenticated<T: BinaryTransport>(transport: &T) -> Result<(), IggyError> {
    if transport.get_state().await != ClientState::Authenticated {
        return Err(IggyError::Unauthenticated);
    }
    Ok(())
}
