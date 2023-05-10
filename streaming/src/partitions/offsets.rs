use crate::partitions::consumer_offset::ConsumerOffset;
use crate::partitions::partition::Partition;
use crate::utils::file;
use shared::error::Error;
use tokio::io::AsyncWriteExt;
use tracing::trace;

impl Partition {
    pub async fn store_offset(&mut self, consumer_id: u32, offset: u64) -> Result<(), Error> {
        trace!(
            "Storing offset: {} for consumer: {}, partition: {}, current: {}...",
            offset,
            consumer_id,
            self.id,
            self.current_offset
        );
        if offset > self.current_offset {
            return Err(Error::InvalidOffset(offset));
        }

        let consumer_offset = self
            .consumer_offsets
            .entry(consumer_id)
            .or_insert(ConsumerOffset {
                offset,
                path: format!("{}/{}", self.consumer_offsets_path, consumer_id),
            });

        let mut file = file::create_file(&consumer_offset.path).await;
        file.write_u64(offset).await?;

        trace!(
            "Stored offset: {} for consumer: {}, partition: {}.",
            offset,
            consumer_id,
            self.id
        );

        Ok(())
    }

    // TODO: Load consumer offsets from disk.
    pub async fn load_offsets(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
