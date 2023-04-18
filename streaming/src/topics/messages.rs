use crate::message::Message;
use crate::topics::topic::Topic;
use shared::error::Error;

impl Topic {
    pub fn get_messages(
        &self,
        partition_id: u32,
        offset: u64,
        count: u32,
    ) -> Result<Vec<&Message>, Error> {
        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        let messages = partition.get_messages(offset, count);
        if messages.is_none() {
            return Err(Error::MessagesNotFound);
        }

        let messages = messages.unwrap();
        if messages.is_empty() {
            return Err(Error::MessagesNotFound);
        }

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
