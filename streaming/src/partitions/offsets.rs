use crate::partitions::consumer_offset::ConsumerOffset;
use crate::partitions::partition::Partition;
use crate::utils::file;
use shared::error::Error;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, trace};

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

    pub async fn load_offsets(&mut self) -> Result<(), Error> {
        let dir_files = fs::read_dir(&self.consumer_offsets_path).await;
        if dir_files.is_err() {
            return Err(Error::CannotReadConsumerOffsets(self.id));
        }

        let mut dir_files = dir_files.unwrap();
        while let Some(dir_entry) = dir_files.next_entry().await.unwrap_or(None) {
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

            let path = path.unwrap();
            let consumer_id = consumer_id.unwrap();
            let mut file = file::open_file(path, false).await;
            let offset = file.read_u64().await?;

            self.consumer_offsets.insert(
                consumer_id,
                ConsumerOffset {
                    offset,
                    path: path.to_string(),
                },
            );

            trace!(
                "Loaded consumer offset: {} for consumer ID: {}, partition ID: {}.",
                offset,
                consumer_id,
                self.id
            );
        }

        Ok(())
    }
}
