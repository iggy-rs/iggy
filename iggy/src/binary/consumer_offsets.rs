use crate::binary::binary_client::BinaryClient;
use crate::binary::mapper;
use crate::bytes_serializable::BytesSerializable;
use crate::command::{GET_CONSUMER_OFFSET_CODE, STORE_CONSUMER_OFFSET_CODE};
use crate::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::error::Error;
use crate::models::offset::Offset;

pub async fn store_consumer_offset(
    client: &dyn BinaryClient,
    command: &StoreConsumerOffset,
) -> Result<(), Error> {
    client
        .send_with_response(STORE_CONSUMER_OFFSET_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn get_consumer_offset(
    client: &dyn BinaryClient,
    command: &GetConsumerOffset,
) -> Result<Offset, Error> {
    let response = client
        .send_with_response(GET_CONSUMER_OFFSET_CODE, &command.as_bytes())
        .await?;
    mapper::map_offset(&response)
}
