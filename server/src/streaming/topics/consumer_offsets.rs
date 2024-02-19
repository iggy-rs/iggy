use crate::streaming::partitions::partition::Partition;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::topics::topic::Topic;
use iggy::error::IggyError;
use iggy::locking::IggySharedMut;
use iggy::locking::IggySharedMutFn;
use iggy::models::consumer_offset_info::ConsumerOffsetInfo;

impl Topic {
    pub async fn store_consumer_offset(
        &self,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        let partition = self.resolve_partition(consumer).await?;
        let partition = partition.read().await;
        partition.store_consumer_offset(consumer, offset).await
    }

    pub async fn get_consumer_offset(
        &self,
        consumer: PollingConsumer,
    ) -> Result<ConsumerOffsetInfo, IggyError> {
        let partition = self.resolve_partition(consumer).await?;
        let partition = partition.read().await;
        let offset = partition.get_consumer_offset(consumer).await?;
        Ok(ConsumerOffsetInfo {
            partition_id: partition.partition_id,
            current_offset: partition.current_offset,
            stored_offset: offset,
        })
    }

    async fn resolve_partition(
        &self,
        consumer: PollingConsumer,
    ) -> Result<&IggySharedMut<Partition>, IggyError> {
        let partition_id = match consumer {
            PollingConsumer::Consumer(_, partition_id) => Ok(partition_id),
            PollingConsumer::ConsumerGroup(consumer_group_id, member_id) => {
                let consumer_group = self
                    .get_consumer_group_by_id(consumer_group_id)?
                    .read()
                    .await;
                consumer_group.get_current_partition_id(member_id).await
            }
        }?;

        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(IggyError::PartitionNotFound(
                partition_id,
                self.topic_id,
                self.stream_id,
            ));
        }

        let partition = partition.unwrap();
        Ok(partition)
    }
}
