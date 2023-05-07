use crate::config::PartitionConfig;
use crate::message::Message;
use crate::segments::segment::Segment;
use ringbuffer::AllocRingBuffer;
use std::sync::Arc;

#[derive(Debug)]
pub struct Partition {
    pub id: u32,
    pub path: String,
    pub current_offset: u64,
    pub messages: AllocRingBuffer<Arc<Message>>,
    pub unsaved_messages_count: u32,
    pub should_increment_offset: bool,
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
        let mut buffer_capacity = config.messages_buffer as usize;
        if buffer_capacity == 0 {
            buffer_capacity = 1;
        }

        let mut partition = Partition {
            id,
            path,
            messages: AllocRingBuffer::with_capacity(buffer_capacity),
            segments: vec![],
            current_offset: 0,
            unsaved_messages_count: 0,
            should_increment_offset: false,
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
