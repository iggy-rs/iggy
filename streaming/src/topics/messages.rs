use crate::message::Message;
use crate::topics::topic::Topic;
use shared::error::Error;
use std::sync::Arc;
use tracing::trace;

impl Topic {
    pub async fn get_messages(
        &self,
        partition_id: u32,
        kind: u8,
        value: u64,
        count: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        match kind {
            0 => partition.get_messages_by_offset(value, count).await,
            1 => partition.get_messages_by_timestamp(value, count).await,
            _ => Err(Error::InvalidCommand),
        }
    }

    pub async fn append_messages(
        &mut self,
        key_kind: u8,
        key_value: u32,
        messages: Vec<Message>,
    ) -> Result<(), Error> {
        let partition_id = match key_kind {
            0 => key_value,
            1 => self.calculate_partition_id(key_value),
            _ => return Err(Error::InvalidCommand),
        };

        self.append_messages_to_partition(partition_id, messages)
            .await
    }

    async fn append_messages_to_partition(
        &mut self,
        partition_id: u32,
        messages: Vec<Message>,
    ) -> Result<(), Error> {
        let partition = self.partitions.get_mut(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        partition.append_messages(messages).await?;
        Ok(())
    }

    fn calculate_partition_id(&self, key: u32) -> u32 {
        let partition_id = key % self.partitions.len() as u32;
        trace!("Calculated partition ID: {} for key: {}", partition_id, key);
        partition_id
    }
}
