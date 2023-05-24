use crate::message::Message;
use crate::topics::topic::Topic;
use shared::error::Error;
use shared::messages::poll_messages::Kind;
use shared::messages::send_messages::KeyKind;
use std::sync::Arc;
use tracing::trace;

// TODO: Resolve partition ID by consumer group if provided.
impl Topic {
    pub async fn get_messages(
        &self,
        consumer_id: u32,
        partition_id: u32,
        kind: Kind,
        value: u64,
        count: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        match kind {
            Kind::Offset => partition.get_messages_by_offset(value, count).await,
            Kind::Timestamp => partition.get_messages_by_timestamp(value, count).await,
            Kind::First => partition.get_first_messages(count).await,
            Kind::Last => partition.get_last_messages(count).await,
            Kind::Next => partition.get_next_messages(consumer_id, count).await,
        }
    }

    pub async fn append_messages(
        &mut self,
        key_kind: KeyKind,
        key_value: u32,
        messages: Vec<Message>,
    ) -> Result<(), Error> {
        let partition_id = match key_kind {
            KeyKind::PartitionId => key_value,
            KeyKind::CalculatePartitionId => self.calculate_partition_id(key_value),
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
