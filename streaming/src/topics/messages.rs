use crate::message::Message;
use crate::storage::SegmentStorage;
use crate::topics::topic::Topic;
use ringbuffer::RingBufferWrite;
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
        let partition = partition.read().await;
        match kind {
            Kind::Offset => partition.get_messages_by_offset(value, count).await,
            Kind::Timestamp => partition.get_messages_by_timestamp(value, count).await,
            Kind::First => partition.get_first_messages(count).await,
            Kind::Last => partition.get_last_messages(count).await,
            Kind::Next => partition.get_next_messages(consumer_id, count).await,
        }
    }

    pub async fn append_messages(
        &self,
        key_kind: KeyKind,
        key_value: u32,
        messages: Vec<Message>,
        storage: Arc<dyn SegmentStorage>,
    ) -> Result<(), Error> {
        let partition_id = match key_kind {
            KeyKind::PartitionId => key_value,
            KeyKind::EntityId => self.calculate_partition_id(key_value),
        };

        self.append_messages_to_partition(partition_id, messages, storage)
            .await
    }

    async fn append_messages_to_partition(
        &self,
        partition_id: u32,
        messages: Vec<Message>,
        storage: Arc<dyn SegmentStorage>,
    ) -> Result<(), Error> {
        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        let mut partition = partition.write().await;
        partition.append_messages(messages, storage).await?;
        Ok(())
    }

    fn calculate_partition_id(&self, entity_id: u32) -> u32 {
        let partition_id = entity_id % self.partitions.len() as u32;
        trace!(
            "Calculated partition ID: {} for key: {}",
            partition_id,
            entity_id
        );
        partition_id
    }

    pub(crate) async fn load_messages_to_cache(&mut self) -> Result<(), Error> {
        let messages_buffer_size = self.config.partition.messages_buffer as u64;
        if messages_buffer_size == 0 {
            return Ok(());
        }

        for (_, partition) in self.partitions.iter_mut() {
            let mut partition = partition.write().await;
            if partition.segments.is_empty() {
                trace!("No segments found for partition with ID: {}", partition.id);
                continue;
            }

            let end_offset = partition.segments.last().unwrap().current_offset;
            let start_offset = if end_offset + 1 >= messages_buffer_size {
                end_offset + 1 - messages_buffer_size
            } else {
                0
            };

            let messages_count = (end_offset - start_offset + 1) as u32;
            trace!(
                "Loading {} messages for partition with ID: {} for topic with ID: {} and stream with ID: {} from offset: {} to offset: {}...",
                messages_count,
                partition.id,
                partition.topic_id,
                partition.stream_id,
                start_offset,
                end_offset
            );

            let messages = partition
                .get_messages_by_offset(start_offset, messages_count)
                .await?;

            if partition.messages.is_some() {
                let partition_messages = partition.messages.as_mut().unwrap();
                for message in messages {
                    partition_messages.push(message);
                }
            }

            trace!(
                "Loaded {} messages for partition with ID: {} for topic with ID: {} and stream with ID: {} from offset: {} to offset: {}.",
                messages_count,
                partition.id,
                partition.topic_id,
                partition.stream_id,
                start_offset,
                end_offset
            );
        }

        Ok(())
    }
}
