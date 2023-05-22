use crate::config::PartitionConfig;
use crate::message::Message;
use crate::segments::segment::Segment;
use ringbuffer::AllocRingBuffer;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct Partition {
    pub id: u32,
    pub path: String,
    pub offsets_path: String,
    pub consumer_offsets_path: String,
    pub current_offset: u64,
    pub messages: Option<AllocRingBuffer<Arc<Message>>>,
    pub unsaved_messages_count: u32,
    pub should_increment_offset: bool,
    pub(crate) consumer_offsets: HashMap<u32, u64>,
    pub(crate) consumer_offsets_paths: HashMap<u32, String>,
    pub(crate) segments: Vec<Segment>,
    pub(crate) config: Arc<PartitionConfig>,
}

impl Partition {
    pub fn empty(id: u32, topic_path: &str, config: Arc<PartitionConfig>) -> Partition {
        Partition::create(id, topic_path, false, config)
    }

    pub fn create(
        id: u32,
        topic_path: &str,
        with_segment: bool,
        config: Arc<PartitionConfig>,
    ) -> Partition {
        let path = Self::get_path(id, topic_path);
        let offsets_path = Self::get_offsets_path(&path);
        let consumer_offsets_path = Self::get_consumer_offsets_path(&offsets_path);
        let mut partition = Partition {
            id,
            path,
            offsets_path,
            consumer_offsets_path,
            messages: match config.messages_buffer {
                0 => None,
                _ => Some(AllocRingBuffer::with_capacity(
                    config.messages_buffer as usize,
                )),
            },
            segments: vec![],
            current_offset: 0,
            unsaved_messages_count: 0,
            should_increment_offset: false,
            consumer_offsets: HashMap::new(),
            consumer_offsets_paths: HashMap::new(),
            config,
        };

        if with_segment {
            let segment = Segment::create(id, 0, &partition.path, partition.config.segment.clone());
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use ringbuffer::RingBuffer;

    #[test]
    fn should_be_created_with_a_single_segment_given_valid_parameters() {
        let id = 1;
        let topic_path = "/topics/1";
        let with_segment = true;
        let config = Arc::new(PartitionConfig::default());
        let path = Partition::get_path(id, topic_path);
        let offsets_path = Partition::get_offsets_path(&path);
        let consumer_offsets_path = Partition::get_consumer_offsets_path(&offsets_path);
        let messages_buffer_capacity = config.messages_buffer as usize;

        let partition = Partition::create(id, topic_path, with_segment, config);

        assert_eq!(partition.id, id);
        assert_eq!(partition.path, path);
        assert_eq!(partition.offsets_path, offsets_path);
        assert_eq!(partition.consumer_offsets_path, consumer_offsets_path);
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
        assert!(partition.consumer_offsets.is_empty());
        assert!(partition.consumer_offsets_paths.is_empty());
    }

    #[test]
    fn should_not_initialize_messages_buffer_given_zero_capacity() {
        let partition = Partition::create(
            1,
            "/topics/1",
            true,
            Arc::new(PartitionConfig {
                messages_buffer: 0,
                ..Default::default()
            }),
        );
        assert!(partition.messages.is_none());
    }

    #[test]
    fn should_not_initialize_segments_given_false_with_segment_parameter() {
        let partition =
            Partition::create(1, "/topics/1", false, Arc::new(PartitionConfig::default()));
        assert!(partition.segments.is_empty());
    }
}
