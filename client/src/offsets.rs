use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::offsets::get_offset::GetOffset;
use iggy::offsets::store_offset::StoreOffset;
use tracing::info;

pub async fn get_offset(command: &GetOffset, client: &dyn Client) -> Result<(), ClientError> {
    let offset = client.get_offset(command).await?;
    info!("Offset: {:#?}", offset);
    Ok(())
}

pub async fn store_offset(command: &StoreOffset, client: &dyn Client) -> Result<(), ClientError> {
    client.store_offset(command).await?;
    Ok(())
}
