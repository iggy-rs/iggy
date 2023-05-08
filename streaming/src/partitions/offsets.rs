use crate::partitions::partition::Partition;
use shared::error::Error;
use tracing::trace;

impl Partition {
    pub async fn store_offset(&mut self, consumer_id: u32, offset: u64) -> Result<(), Error> {
        if offset > self.current_offset {
            return Err(Error::InvalidOffset(offset));
        }

        self.consumer_offsets.entry(consumer_id).or_insert(offset);
        trace!(
            "Stored offset: {} for consumer: {}, partition: {}.",
            offset,
            consumer_id,
            self.id
        );

        // TODO: Store consumer offset on disk.

        Ok(())
    }
}
