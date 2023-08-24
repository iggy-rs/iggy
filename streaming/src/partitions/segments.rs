use crate::partitions::partition::Partition;
use iggy::error::Error;

impl Partition {
    // TODO: Remove the cached messages if exist
    pub async fn delete_segment(&mut self, start_offset: u64) -> Result<(), Error> {
        let segment = self
            .segments
            .iter()
            .find(|s| s.start_offset == start_offset);
        if let None = segment {
            return Err(Error::SegmentNotFound);
        }

        let segment = segment.unwrap();
        self.storage.segment.delete(segment).await?;
        Ok(())
    }
}
