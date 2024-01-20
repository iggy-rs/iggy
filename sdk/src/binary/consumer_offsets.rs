use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::client::ConsumerOffsetClient;
use crate::command::{GET_CONSUMER_OFFSET_CODE, STORE_CONSUMER_OFFSET_CODE};
use crate::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::error::IggyError;
use crate::models::consumer_offset_info::ConsumerOffsetInfo;

#[async_trait::async_trait]
impl<B: BinaryClient> ConsumerOffsetClient for B {
    async fn store_consumer_offset(&self, command: &StoreConsumerOffset) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(STORE_CONSUMER_OFFSET_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }

    async fn get_consumer_offset(
        &self,
        command: &GetConsumerOffset,
    ) -> Result<ConsumerOffsetInfo, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_CONSUMER_OFFSET_CODE, &command.as_bytes())
            .await?;
        mapper::map_consumer_offset(&response)
    }
}
