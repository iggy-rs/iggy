use crate::streaming::partitions::partition::Partition;
use crate::streaming::segments::segment::Segment;
use iggy::error::IggyError;
use tracing::info;

pub struct DeletedSegment {
    pub end_offset: u64,
    pub messages_count: u64,
}

impl Partition {
    pub fn get_segments_count(&self) -> u32 {
        self.segments.len() as u32
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

    pub async fn add_persisted_segment(&mut self, start_offset: u64) -> Result<(), IggyError> {
        info!(
            "Creating the new segment for partition with ID: {}, stream with ID: {}, topic with ID: {}...",
            self.partition_id, self.stream_id, self.topic_id
        );
        let new_segment = Segment::create(
            self.stream_id,
            self.topic_id,
            self.partition_id,
            start_offset,
            self.config.clone(),
            self.storage.clone(),
            self.message_expiry,
            self.size_of_parent_stream.clone(),
            self.size_of_parent_topic.clone(),
            self.size_bytes.clone(),
            self.messages_count_of_parent_stream.clone(),
            self.messages_count_of_parent_topic.clone(),
            self.messages_count.clone(),
        );
        new_segment.persist().await?;
        self.segments.push(new_segment);
        Ok(())
    }

    pub async fn delete_segment(&mut self, start_offset: u64) -> Result<DeletedSegment, IggyError> {
        let deleted_segment;
        {
            let segment = self
                .segments
                .iter()
                .find(|s| s.start_offset == start_offset);
            if segment.is_none() {
                return Err(IggyError::SegmentNotFound);
            }

            let segment = segment.unwrap();
            self.storage.segment.delete(segment).await?;

            deleted_segment = DeletedSegment {
                end_offset: segment.end_offset,
                messages_count: segment.get_messages_count(),
            };
        }

        self.segments.retain(|s| s.start_offset != start_offset);
        Ok(deleted_segment)
    }
}
