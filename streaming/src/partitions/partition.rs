use crate::config::PartitionConfig;
use crate::segments::segment::Segment;
use std::sync::Arc;

#[derive(Debug)]
pub struct Partition {
    pub id: u32,
    pub path: String,
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
        let path = format!("{}/{:0>10}", topic_path, id);
        let mut partition = Partition {
            id,
            path,
            segments: vec![],
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
