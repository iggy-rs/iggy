use crate::binary::binary_client::{BinaryClient, ClientState};
use crate::error::Error;

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

async fn fail_if_not_authenticated(client: &dyn BinaryClient) -> Result<(), Error> {
    if client.get_state().await != ClientState::Authenticated {
        return Err(Error::Unauthenticated);
    }
    Ok(())
}
