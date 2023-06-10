use crate::partitions::partition::{ConsumerOffset, Partition};
use crate::utils::file;
use shared::error::Error;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{error, trace};

impl Partition {
    pub async fn store_offset(&self, consumer_id: u32, offset: u64) -> Result<(), Error> {
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

        // This scope is required to avoid the potential deadlock by acquiring read lock and then write lock.
        {
            let consumer_offsets = self.consumer_offsets.read().await;
            let consumer_offset = consumer_offsets.offsets.get(&consumer_id);
            if let Some(consumer_offset) = consumer_offset {
                let mut consumer_offset = consumer_offset.write().await;
                consumer_offset.offset = offset;
                self.store_offset_in_file(&consumer_offset.path, consumer_id, offset)
                    .await?;
                return Ok(());
            }
        }

        let mut consumer_offsets = self.consumer_offsets.write().await;
        let path = format!("{}/{}", self.consumer_offsets_path, consumer_id);
        self.store_offset_in_file(&path, consumer_id, offset)
            .await?;
        consumer_offsets
            .offsets
            .insert(consumer_id, RwLock::new(ConsumerOffset { offset, path }));

        Ok(())
    }

    async fn store_offset_in_file(
        &self,
        path: &str,
        consumer_id: u32,
        offset: u64,
    ) -> Result<(), Error> {
        let mut file = file::create_file(path).await?;
        file.write_u64(offset).await?;

        trace!(
            "Stored offset: {} for consumer: {}, partition: {}.",
            offset,
            consumer_id,
            self.id
        );

        Ok(())
    }

    pub async fn load_offsets(&mut self) -> Result<(), Error> {
        trace!(
                "Loading consumer offsets for partition with ID: {} for topic with ID: {} and stream with ID: {}...",
                self.id,
                self.topic_id,
                self.stream_id
            );

        let dir_entries = fs::read_dir(&self.consumer_offsets_path).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadConsumerOffsets(self.id));
        }

        let mut dir_entries = dir_entries.unwrap();
        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
            let metadata = dir_entry.metadata().await;
            if metadata.is_err() || metadata.unwrap().is_dir() {
                continue;
            }

            let name = dir_entry.file_name().into_string().unwrap();
            let consumer_id = name.parse::<u32>();
            if consumer_id.is_err() {
                error!("Invalid consumer ID file with name: '{}'.", name);
                continue;
            }

            let path = dir_entry.path();
            let path = path.to_str();
            if path.is_none() {
                error!("Invalid consumer ID path for file with name: '{}'.", name);
                continue;
            }

            let path = path.unwrap().to_string();
            let consumer_id = consumer_id.unwrap();
            let mut file = file::open_file(&path, false).await?;
            let offset = file.read_u64_le().await?;

            let mut consumer_offsets = self.consumer_offsets.write().await;
            consumer_offsets
                .offsets
                .insert(consumer_id, RwLock::new(ConsumerOffset { offset, path }));

            trace!(
                "Loaded consumer offset: {} for consumer ID: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                offset,
                consumer_id,
                self.id,
                self.topic_id,
                self.stream_id
            );
        }

        Ok(())
    }
}
