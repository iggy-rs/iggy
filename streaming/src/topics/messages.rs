use crate::message::Message;
use crate::topics::topic::Topic;
use shared::error::Error;
use std::sync::Arc;

impl Topic {
    pub async fn get_messages(
        &self,
        partition_id: u32,
        offset: u64,
        count: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        let messages = partition.get_messages(offset, count).await?;
        Ok(messages)
    }

    pub async fn append_messages(
        &mut self,
        partition_id: u32,
        message: Message,
    ) -> Result<(), Error> {
        let partition = self.partitions.get_mut(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        partition.append_messages(message).await?;
        Ok(())
    }
}
