use crate::config::PartitionConfig;
use crate::message::Message;
use crate::segments::segment::Segment;
use crate::storage::SystemStorage;
use ringbuffer::AllocRingBuffer;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct Partition {
    pub stream_id: u32,
    pub topic_id: u32,
    pub id: u32,
    pub path: String,
    pub offsets_path: String,
    pub consumer_offsets_path: String,
    pub consumer_group_offsets_path: String,
    pub current_offset: u64,
    pub messages: Option<AllocRingBuffer<Arc<Message>>>,
    pub message_ids: Option<HashMap<u128, bool>>,
    pub unsaved_messages_count: u32,
    pub should_increment_offset: bool,
    pub(crate) consumer_offsets: RwLock<ConsumerOffsets>,
    pub(crate) consumer_group_offsets: RwLock<ConsumerOffsets>,
    pub(crate) segments: Vec<Segment>,
    pub(crate) config: Arc<PartitionConfig>,
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
        id: u32,
        topic_path: &str,
        config: Arc<PartitionConfig>,
        storage: Arc<SystemStorage>,
    ) -> Partition {
        Partition::create(stream_id, topic_id, id, topic_path, false, config, storage)
    }

    pub fn create(
        stream_id: u32,
        topic_id: u32,
        id: u32,
        topic_path: &str,
        with_segment: bool,
        config: Arc<PartitionConfig>,
        storage: Arc<SystemStorage>,
    ) -> Partition {
        let path = Self::get_path(id, topic_path);
        let offsets_path = Self::get_offsets_path(&path);
        let consumer_offsets_path = Self::get_consumer_offsets_path(&offsets_path);
        let consumer_group_offsets_path = Self::get_consumer_group_offsets_path(&offsets_path);
        let mut partition = Partition {
            stream_id,
            topic_id,
            id,
            path,
            offsets_path,
            consumer_offsets_path,
            consumer_group_offsets_path,
            messages: match config.messages_buffer {
                0 => None,
                _ => Some(AllocRingBuffer::with_capacity(
                    config.messages_buffer as usize,
                )),
            },
            message_ids: match config.deduplicate_messages {
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
        };

        if with_segment {
            let segment = Segment::create(
                stream_id,
                topic_id,
                id,
                0,
                &partition.path,
                partition.config.segment.clone(),
                partition.storage.clone(),
            );
            partition.segments.push(segment);
        }

        partition
    }

    pub fn get_segments(&self) -> &Vec<Segment> {
        &self.segments
    }

    pub fn get_segments_mut(&mut self) -> &mut Vec<Segment> {
        &mut self.segments
    }

    fn get_path(id: u32, topic_path: &str) -> String {
        format!("{}/{}", topic_path, id)
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
    use crate::config::PartitionConfig;
    use crate::partitions::partition::Partition;
    use crate::storage::tests::get_test_system_storage;
    use ringbuffer::RingBuffer;
    use std::sync::Arc;

    #[test]
    fn should_be_created_with_a_single_segment_given_valid_parameters() {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let id = 3;
        let topic_path = "/topics/2";
        let with_segment = true;
        let config = Arc::new(PartitionConfig::default());
        let path = Partition::get_path(id, topic_path);
        let offsets_path = Partition::get_offsets_path(&path);
        let consumer_offsets_path = Partition::get_consumer_offsets_path(&offsets_path);
        let consumer_group_offsets_path = Partition::get_consumer_group_offsets_path(&offsets_path);
        let messages_buffer_capacity = config.messages_buffer as usize;

        let partition = Partition::create(
            stream_id,
            topic_id,
            id,
            topic_path,
            with_segment,
            config,
            storage,
        );

        assert_eq!(partition.stream_id, stream_id);
        assert_eq!(partition.topic_id, topic_id);
        assert_eq!(partition.id, id);
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
    }

    #[test]
    fn should_not_initialize_messages_buffer_given_zero_capacity() {
        let storage = Arc::new(get_test_system_storage());
        let partition = Partition::create(
            1,
            1,
            1,
            "/topics/1",
            true,
            Arc::new(PartitionConfig {
                messages_buffer: 0,
                ..Default::default()
            }),
            storage,
        );
        assert!(partition.messages.is_none());
    }

    #[test]
    fn should_not_initialize_segments_given_false_with_segment_parameter() {
        let storage = Arc::new(get_test_system_storage());
        let partition = Partition::create(
            1,
            1,
            1,
            "/topics/1",
            false,
            Arc::new(PartitionConfig::default()),
            storage,
        );
        assert!(partition.segments.is_empty());
    }
}
