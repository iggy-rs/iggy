use crate::topics::topic::Topic;
use shared::error::Error;

impl Topic {
    pub async fn store_offset(
        &mut self,
        consumer_id: u32,
        partition_id: u32,
        offset: u64,
    ) -> Result<(), Error> {
        let partition = self.partitions.get_mut(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        partition.unwrap().store_offset(consumer_id, offset).await
    }
}
