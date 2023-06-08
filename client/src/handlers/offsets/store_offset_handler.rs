use crate::client_error::ClientError;
use sdk::client::Client;
use shared::offsets::store_offset::StoreOffset;

pub async fn handle(command: StoreOffset, client: &dyn Client) -> Result<(), ClientError> {
    client.store_offset(command).await?;
    Ok(())
}
