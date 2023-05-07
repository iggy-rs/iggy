use crate::client_error::ClientError;
use sdk::client::ConnectedClient;
use shared::offsets::store_offset::StoreOffset;

pub async fn handle(command: StoreOffset, client: &mut ConnectedClient) -> Result<(), ClientError> {
    client.store_offset(&command).await?;
    Ok(())
}
