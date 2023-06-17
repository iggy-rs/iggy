use crate::topics::topic::Topic;
use sdk::error::Error;

impl Topic {
    pub async fn store_offset(
        &self,
        consumer_id: u32,
        partition_id: u32,
        offset: u64,
    ) -> Result<(), Error> {
        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        let partition = partition.read().await;
        partition.store_offset(consumer_id, offset).await
    }

    pub async fn get_offset(&self, consumer_id: u32, partition_id: u32) -> Result<u64, Error> {
        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        let partition = partition.read().await;
        partition.get_offset(consumer_id).await
    }
}
