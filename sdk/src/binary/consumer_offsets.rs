use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::client::ConsumerOffsetClient;
use crate::consumer::Consumer;
use crate::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::models::consumer_offset_info::ConsumerOffsetInfo;

#[async_trait::async_trait]
impl<B: BinaryClient> ConsumerOffsetClient for B {
    async fn store_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&StoreConsumerOffset {
            consumer: consumer.clone(),
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partition_id,
            offset,
        })
        .await?;
        Ok(())
    }

    async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&GetConsumerOffset {
                consumer: consumer.clone(),
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partition_id,
            })
            .await?;
        if response.is_empty() {
            return Ok(None);
        }

        mapper::map_consumer_offset(response).map(Some)
    }
}
