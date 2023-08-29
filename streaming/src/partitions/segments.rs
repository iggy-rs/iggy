use crate::partitions::partition::Partition;
use crate::segments::segment::Segment;
use iggy::error::Error;
use tracing::info;

impl Partition {
    pub async fn add_persisted_segment(&mut self, start_offset: u64) -> Result<(), Error> {
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
        );
        new_segment.persist().await?;
        self.segments.push(new_segment);
        Ok(())
    }

    pub async fn delete_segment(&mut self, start_offset: u64) -> Result<&Segment, Error> {
        let segment = self
            .segments
            .iter()
            .find(|s| s.start_offset == start_offset);
        if segment.is_none() {
            return Err(Error::SegmentNotFound);
        }

        let segment = segment.unwrap();
        self.storage.segment.delete(segment).await?;
        Ok(segment)
    }
}
