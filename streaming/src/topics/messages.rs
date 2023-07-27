use crate::message::Message;
use crate::polling_consumer::PollingConsumer;
use crate::topics::topic::Topic;
use crate::utils::hash;
use iggy::error::Error;
use iggy::messages::poll_messages::Kind;
use iggy::messages::send_messages::{Key, KeyKind};
use ringbuffer::RingBufferWrite;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::trace;

impl Topic {
    pub async fn get_messages(
        &self,
        consumer: PollingConsumer,
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
            Kind::Next => partition.get_next_messages(consumer, count).await,
        }
    }

    pub async fn append_messages(&self, key: &Key, messages: Vec<Message>) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let partition_id = match key.kind {
            KeyKind::None => self.get_next_partition_id(),
            KeyKind::PartitionId => {
                u32::from_le_bytes(key.value[..key.length as usize].try_into()?)
            }
            KeyKind::EntityId => self.calculate_partition_id_by_entity_id_hash(&key.value),
        };

        self.append_messages_to_partition(partition_id, messages)
            .await
    }

    async fn append_messages_to_partition(
        &self,
        partition_id: u32,
        messages: Vec<Message>,
    ) -> Result<(), Error> {
        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        let mut partition = partition.write().await;
        partition.append_messages(messages).await?;
        Ok(())
    }

    fn get_next_partition_id(&self) -> u32 {
        let mut partition_id = self.current_partition_id.fetch_add(1, Ordering::SeqCst);
        let partitions_count = self.partitions.len() as u32;
        if partition_id > partitions_count {
            partition_id = 1;
            self.current_partition_id
                .swap(partition_id + 1, Ordering::SeqCst);
        }
        trace!("Next partition ID: {}", partition_id);
        partition_id
    }

    fn calculate_partition_id_by_entity_id_hash(&self, entity_id: &[u8]) -> u32 {
        let entity_id_hash = hash::calculate(entity_id);
        let partitions_count = self.partitions.len() as u128;
        let mut partition_id = (entity_id_hash % partitions_count) as u32;
        if partition_id == 0 {
            partition_id = partitions_count as u32;
        }
        trace!(
            "Calculated partition ID: {} for entity ID: {:?}, hash: {}",
            partition_id,
            entity_id,
            entity_id_hash
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TopicConfig;
    use crate::storage::tests::get_test_system_storage;
    use bytes::Bytes;
    use ringbuffer::RingBufferExt;

    #[tokio::test]
    async fn given_partition_id_key_messages_should_be_appended_only_to_the_chosen_partition() {
        let partition_id = 1;
        let key = Key::partition_id(partition_id);
        let partitions_count = 3;
        let messages_count = 1000;
        let topic = init_topic(partitions_count);

        for entity_id in 1..=messages_count {
            let payload = Bytes::from("test");
            let messages = vec![Message::empty(1, entity_id as u128, payload, 1)];
            topic.append_messages(&key, messages).await.unwrap();
        }

        let partitions = topic.get_partitions();
        assert_eq!(partitions.len(), partitions_count as usize);
        for partition in partitions {
            let partition = partition.read().await;
            let messages = partition.messages.as_ref().unwrap().to_vec();
            if partition.id == partition_id {
                assert_eq!(messages.len() as u32, messages_count);
            } else {
                assert_eq!(messages.len() as u32, 0);
            }
        }
    }

    #[tokio::test]
    async fn given_entity_id_key_messages_should_be_appended_to_the_calculated_partitions() {
        let partitions_count = 3;
        let messages_count = 1000;
        let topic = init_topic(partitions_count);

        for entity_id in 1..=messages_count {
            let key = Key::entity_id_u32(entity_id);
            let payload = Bytes::from("test");
            let messages = vec![Message::empty(1, entity_id as u128, payload, 1)];
            topic.append_messages(&key, messages).await.unwrap();
        }

        let mut read_messages_count = 0;
        let partitions = topic.get_partitions();
        assert_eq!(partitions.len(), partitions_count as usize);
        for partition in partitions {
            let partition = partition.read().await;
            let messages = partition.messages.as_ref().unwrap().to_vec();
            read_messages_count += messages.len();
            assert!(messages.len() < messages_count as usize);
        }

        assert_eq!(read_messages_count, messages_count as usize);
    }

    #[test]
    fn given_multiple_partitions_calculate_next_partition_id_should_return_next_partition_id_using_round_robin(
    ) {
        let partitions_count = 3;
        let messages_count = 1000;
        let topic = init_topic(partitions_count);

        let mut expected_partition_id = 0;
        for _ in 1..=messages_count {
            let partition_id = topic.get_next_partition_id();
            expected_partition_id += 1;
            if expected_partition_id > partitions_count {
                expected_partition_id = 1;
            }

            assert_eq!(partition_id, expected_partition_id);
        }
    }

    #[test]
    fn given_multiple_partitions_calculate_partition_id_by_hash_should_return_next_partition_id() {
        let partitions_count = 3;
        let messages_count = 1000;
        let topic = init_topic(partitions_count);

        for entity_id in 1..=messages_count {
            let key = Key::entity_id_u32(entity_id);
            let partition_id = topic.calculate_partition_id_by_entity_id_hash(&key.value);
            let entity_id_hash = hash::calculate(&key.value);
            let mut expected_partition_id = (entity_id_hash % partitions_count as u128) as u32;
            if expected_partition_id == 0 {
                expected_partition_id = partitions_count;
            }

            assert_eq!(partition_id, expected_partition_id);
        }
    }

    fn init_topic(partitions_count: u32) -> Topic {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let id = 2;
        let topics_path = "/topics";
        let name = "test";
        let config = Arc::new(TopicConfig::default());

        Topic::create(
            stream_id,
            id,
            name,
            partitions_count,
            topics_path,
            config,
            storage,
        )
    }
}
