use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::topics::topic::Topic;
use iggy::consumer::Consumer;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::models::consumer_offset_info::ConsumerOffsetInfo;

impl Topic {
    pub async fn store_consumer_offset(
        &self,
        consumer: Consumer,
        offset: u64,
        partition_id: Option<u32>,
        client_id: u32,
    ) -> Result<(), IggyError> {
        let (polling_consumer, partition_id) = self
            .resolve_consumer_with_partition_id(&consumer, client_id, partition_id, false)
            .await?;
        let partition = self.get_partition(partition_id)?;
        let partition = partition.read().await;
        partition
            .store_consumer_offset(polling_consumer, offset)
            .await
    }

    pub async fn store_consumer_offset_internal(
        &self,
        consumer: PollingConsumer,
        offset: u64,
        partition_id: u32,
    ) -> Result<(), IggyError> {
        let partition = self.get_partition(partition_id)?;
        let partition = partition.read().await;
        partition.store_consumer_offset(consumer, offset).await
    }

    pub async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        partition_id: Option<u32>,
        client_id: u32,
    ) -> Result<ConsumerOffsetInfo, IggyError> {
        let (polling_consumer, partition_id) = self
            .resolve_consumer_with_partition_id(consumer, client_id, partition_id, false)
            .await?;
        let partition = self.get_partition(partition_id)?;
        let partition = partition.read().await;
        let offset = partition.get_consumer_offset(polling_consumer).await?;
        Ok(ConsumerOffsetInfo {
            partition_id: partition.partition_id,
            current_offset: partition.current_offset,
            stored_offset: offset,
        })
    }
}
