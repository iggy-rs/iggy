use crate::binary::binary_client::{BinaryClient, BinaryClientV2};
use crate::binary::{fail_if_not_authenticated, mapper, BinaryTransport};
use crate::bytes_serializable::BytesSerializable;
use crate::client::ConsumerOffsetClient;
use crate::client_v2::ConsumerOffsetClientV2;
use crate::command::{GET_CONSUMER_OFFSET_CODE, STORE_CONSUMER_OFFSET_CODE};
use crate::consumer::Consumer;
use crate::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::models::consumer_offset_info::ConsumerOffsetInfo;

#[async_trait::async_trait]
impl<B: BinaryClient> ConsumerOffsetClient for B {
    async fn store_consumer_offset(&self, command: &StoreConsumerOffset) -> Result<(), IggyError> {
        store_consumer_offset(self, command).await
    }

    async fn get_consumer_offset(
        &self,
        command: &GetConsumerOffset,
    ) -> Result<ConsumerOffsetInfo, IggyError> {
        get_consumer_offset(self, command).await
    }
}

#[async_trait::async_trait]
impl<B: BinaryClientV2> ConsumerOffsetClientV2 for B {
    async fn store_consumer_offset(
        &self,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        store_consumer_offset(
            self,
            &StoreConsumerOffset {
                consumer,
                stream_id,
                topic_id,
                partition_id,
                offset,
            },
        )
        .await
    }

    async fn get_consumer_offset(
        &self,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
    ) -> Result<ConsumerOffsetInfo, IggyError> {
        get_consumer_offset(
            self,
            &GetConsumerOffset {
                consumer,
                stream_id,
                topic_id,
                partition_id,
            },
        )
        .await
    }
}

async fn store_consumer_offset<T: BinaryTransport>(
    transport: &T,
    command: &StoreConsumerOffset,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(STORE_CONSUMER_OFFSET_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn get_consumer_offset<T: BinaryTransport>(
    transport: &T,
    command: &GetConsumerOffset,
) -> Result<ConsumerOffsetInfo, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_CONSUMER_OFFSET_CODE, command.as_bytes())
        .await?;
    mapper::map_consumer_offset(response)
}
