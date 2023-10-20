use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::command::{GET_CONSUMER_OFFSET_CODE, STORE_CONSUMER_OFFSET_CODE};
use crate::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::error::Error;
use crate::models::consumer_offset_info::ConsumerOffsetInfo;

pub async fn store_consumer_offset(
    client: &dyn BinaryClient,
    command: &StoreConsumerOffset,
) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(STORE_CONSUMER_OFFSET_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn get_consumer_offset(
    client: &dyn BinaryClient,
    command: &GetConsumerOffset,
) -> Result<ConsumerOffsetInfo, Error> {
    fail_if_not_authenticated(client).await?;
    let response = client
        .send_with_response(GET_CONSUMER_OFFSET_CODE, &command.as_bytes())
        .await?;
    mapper::map_consumer_offset(&response)
}
