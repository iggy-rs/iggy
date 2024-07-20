#[allow(deprecated)]
use crate::binary::binary_client::BinaryClient;
use crate::binary::fail_if_not_authenticated;
use crate::client::PartitionClient;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;

#[async_trait::async_trait]
impl<B: BinaryClient> PartitionClient for B {
    async fn create_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&CreatePartitions {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partitions_count,
        })
        .await?;
        Ok(())
    }

    async fn delete_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&DeletePartitions {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partitions_count,
        })
        .await?;
        Ok(())
    }
}
