use crate::config::SystemConfig;
use crate::segments::segment::Segment;
use crate::storage::SystemStorage;
use iggy::models::messages::Message;
use iggy::utils::timestamp;
use ringbuffer::AllocRingBuffer;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct Partition {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub path: String,
    pub offsets_path: String,
    pub consumer_offsets_path: String,
    pub consumer_group_offsets_path: String,
    pub current_offset: u64,
    pub messages: Option<AllocRingBuffer<Arc<Message>>>,
    pub message_ids: Option<HashMap<u128, bool>>,
    pub unsaved_messages_count: u32,
    pub should_increment_offset: bool,
    pub created_at: u64,
    pub(crate) message_expiry: Option<u32>,
    pub(crate) consumer_offsets: RwLock<ConsumerOffsets>,
    pub(crate) consumer_group_offsets: RwLock<ConsumerOffsets>,
    pub(crate) segments: Vec<Segment>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) storage: Arc<SystemStorage>,
}

#[derive(Debug)]
pub struct ConsumerOffsets {
    pub(crate) offsets: HashMap<u32, RwLock<ConsumerOffset>>,
}

#[derive(Debug)]
pub struct ConsumerOffset {
    pub(crate) consumer_id: u32,
    pub(crate) offset: u64,
    pub(crate) path: String,
}

impl Partition {
    pub fn empty(
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        config: Arc<SystemConfig>,
        storage: Arc<SystemStorage>,
    ) -> Partition {
        Partition::create(
            stream_id,
            topic_id,
            partition_id,
            false,
            config,
            storage,
            None,
        )
    }

    pub fn create(
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        with_segment: bool,
        config: Arc<SystemConfig>,
        storage: Arc<SystemStorage>,
        message_expiry: Option<u32>,
    ) -> Partition {
        let path = config.get_partition_path(stream_id, topic_id, partition_id);
        let offsets_path = Self::get_offsets_path(&path);
        let consumer_offsets_path = Self::get_consumer_offsets_path(&offsets_path);
        let consumer_group_offsets_path = Self::get_consumer_group_offsets_path(&offsets_path);
        let mut partition = Partition {
            stream_id,
            topic_id,
            partition_id,
            path,
            offsets_path,
            consumer_offsets_path,
            consumer_group_offsets_path,
            message_expiry,
            messages: match config.partition.messages_buffer {
                0 => None,
                _ => Some(AllocRingBuffer::new(
                    config.partition.messages_buffer as usize,
                )),
            },
            message_ids: match config.partition.deduplicate_messages {
                true => Some(HashMap::new()),
                false => None,
            },
            segments: vec![],
            current_offset: 0,
            unsaved_messages_count: 0,
            should_increment_offset: false,
            consumer_offsets: RwLock::new(ConsumerOffsets {
                offsets: HashMap::new(),
            }),
            consumer_group_offsets: RwLock::new(ConsumerOffsets {
                offsets: HashMap::new(),
            }),
            config,
            storage,
            created_at: timestamp::get(),
        };

        if with_segment {
            let segment = Segment::create(
                stream_id,
                topic_id,
                partition_id,
                0,
                partition.config.clone(),
                partition.storage.clone(),
                partition.message_expiry,
            );
            partition.segments.push(segment);
        }

        partition
    }

    pub fn get_size_bytes(&self) -> u64 {
        self.segments
            .iter()
            .map(|segment| segment.current_size_bytes as u64)
            .sum()
    }

    pub fn get_messages_count(&self) -> u64 {
        let last_segment = self.segments.last();
        if let Some(last_segment) = last_segment {
            if last_segment.current_size_bytes > 0 {
                return last_segment.current_offset + 1;
            }
        }
        0
    }

    pub fn get_segments(&self) -> &Vec<Segment> {
        &self.segments
    }

    pub fn get_segments_mut(&mut self) -> &mut Vec<Segment> {
        &mut self.segments
    }

    pub async fn get_expired_segments_start_offsets(&self, now: u64) -> Vec<u64> {
        let mut expired_segments = Vec::new();
        for segment in &self.segments {
            if segment.is_expired(now).await {
                expired_segments.push(segment.start_offset);
            }
        }

        expired_segments.sort();
        expired_segments
    }

    fn get_offsets_path(path: &str) -> String {
        format!("{}/offsets", path)
    }

    fn get_consumer_offsets_path(offsets_path: &str) -> String {
        format!("{}/consumers", offsets_path)
    }

    fn get_consumer_group_offsets_path(offsets_path: &str) -> String {
        format!("{}/groups", offsets_path)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{PartitionConfig, SystemConfig};
    use crate::partitions::partition::Partition;
    use crate::storage::tests::get_test_system_storage;
    use ringbuffer::RingBuffer;
    use std::sync::Arc;

    #[test]
    fn should_be_created_with_a_single_segment_given_valid_parameters() {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let with_segment = true;
        let config = Arc::new(SystemConfig::default());
        let path = config.get_partition_path(stream_id, topic_id, partition_id);
        let offsets_path = Partition::get_offsets_path(&path);
        let consumer_offsets_path = Partition::get_consumer_offsets_path(&offsets_path);
        let consumer_group_offsets_path = Partition::get_consumer_group_offsets_path(&offsets_path);
        let messages_buffer_capacity = config.partition.messages_buffer as usize;
        let message_expiry = Some(10);

        let partition = Partition::create(
            stream_id,
            topic_id,
            partition_id,
            with_segment,
            config,
            storage,
            message_expiry,
        );

        assert_eq!(partition.stream_id, stream_id);
        assert_eq!(partition.topic_id, topic_id);
        assert_eq!(partition.partition_id, partition_id);
        assert_eq!(partition.path, path);
        assert_eq!(partition.offsets_path, offsets_path);
        assert_eq!(partition.consumer_offsets_path, consumer_offsets_path);
        assert_eq!(
            partition.consumer_group_offsets_path,
            consumer_group_offsets_path
        );
        assert_eq!(partition.current_offset, 0);
        assert_eq!(partition.unsaved_messages_count, 0);
        assert_eq!(partition.segments.len(), 1);
        assert!(partition.messages.is_some());
        assert_eq!(
            partition.messages.as_ref().unwrap().capacity(),
            messages_buffer_capacity
        );
        assert!(!partition.should_increment_offset);
        assert!(partition.messages.as_ref().unwrap().is_empty());
        let consumer_offsets = partition.consumer_offsets.blocking_read();
        assert!(consumer_offsets.offsets.is_empty());
        assert_eq!(partition.message_expiry, message_expiry);
    }

    #[test]
    fn should_not_initialize_messages_buffer_given_zero_capacity() {
        let storage = Arc::new(get_test_system_storage());
        let partition = Partition::create(
            1,
            1,
            1,
            true,
            Arc::new(SystemConfig {
                partition: PartitionConfig {
                    messages_buffer: 0,
                    ..Default::default()
                },
                ..Default::default()
            }),
            storage,
            None,
        );
        assert!(partition.messages.is_none());
    }

    #[test]
    fn should_not_initialize_segments_given_false_with_segment_parameter() {
        let storage = Arc::new(get_test_system_storage());
        let topic_id = 1;
        let partition = Partition::create(
            1,
            topic_id,
            1,
            false,
            Arc::new(SystemConfig::default()),
            storage,
            None,
        );
        assert!(partition.segments.is_empty());
    }
}
