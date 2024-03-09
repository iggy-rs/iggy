use crate::client::Client;
use crate::error::IggyError;
use crate::write_to::WriteTo;
use async_trait::async_trait;
use bytes::Bytes;

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
pub(crate) trait BinaryClient: Client {
    /// Gets the state of the client.
    async fn get_state(&self) -> ClientState;
    /// Sets the state of the client.
    async fn set_state(&self, state: ClientState);
    /// Sends a command and returns the response.
    async fn send_with_response(
        &self,
        command_code: u32,
        payload: Bytes,
    ) -> Result<Bytes, IggyError>;
    /// Sends a command and returns the response.
    ///
    /// No additional memory allocation is required, and the Command is serialized directly into the transmission stream.
    async fn send_with_response_v2(
        &self,
        _command_code: u32,
        _command: &(dyn WriteTo + Send + Sync),
    ) -> Result<Bytes, IggyError> {
        unimplemented!("send_with_response_v2 is not implemented!")
    }
}
