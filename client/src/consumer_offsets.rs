use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use tracing::info;

pub async fn get_consumer_offset(
    command: &GetConsumerOffset,
    client: &dyn Client,
) -> Result<(), ClientError> {
    let offset = client.get_consumer_offset(command).await?;
    info!("Consumer offset: {:#?}", offset);
    Ok(())
}

pub async fn store_consumer_offset(
    command: &StoreConsumerOffset,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.store_consumer_offset(command).await?;
    Ok(())
}
