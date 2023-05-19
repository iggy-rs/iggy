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
    pub fn create(
        id: u32,
        topic_path: &str,
        with_segment: bool,
        config: Arc<PartitionConfig>,
    ) -> Partition {
        let path = format!("{}/{}", topic_path, id);
        let offsets_path = format!("{}/offsets", path);
        let consumer_offsets_path = format!("{}/consumers", offsets_path);
        let mut buffer_capacity = config.messages_buffer as usize;
        if buffer_capacity == 0 {
            buffer_capacity = 1;
        }

        let mut partition = Partition {
            id,
            path,
            offsets_path,
            consumer_offsets_path,
            messages: match config.messages_buffer {
                0 => None,
                _ => Some(AllocRingBuffer::with_capacity(buffer_capacity)),
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

    pub fn empty(config: Arc<PartitionConfig>) -> Partition {
        Partition::create(0, "", false, config)
    }

    pub fn get_segments(&self) -> &Vec<Segment> {
        &self.segments
    }

    pub fn get_segments_mut(&mut self) -> &mut Vec<Segment> {
        &mut self.segments
    }
}
