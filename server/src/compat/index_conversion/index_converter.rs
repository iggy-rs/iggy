use std::path::Path;
use std::time::Duration;

use crate::compat::index_conversion::COMPONENT;
use crate::streaming::utils::file;
use crate::{server_error::CompatError, streaming::segments::storage::INDEX_SIZE};
use bytes::{BufMut, BytesMut};
use error_set::ResultContext;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::time::{sleep, timeout};
use tracing::{error, info, trace};

pub struct IndexConverter {
    pub index_path: String,
    pub time_index_path: String,
}

impl IndexConverter {
    pub fn new(index_path: String, time_index_path: String) -> Self {
        Self {
            index_path,
            time_index_path,
        }
    }

    pub async fn migrate(&self) -> Result<(), CompatError> {
        let indexes = self.convert_indexes().await?;
        self.replace_with_converted(indexes).await?;

        Ok(())
    }

    pub async fn needs_migration(&self) -> Result<bool, CompatError> {
        Ok(file::exists(&self.time_index_path).await?)
    }

    async fn convert_indexes(&self) -> Result<Vec<u8>, CompatError> {
        let index_file = file::open(&self.index_path).await?;
        let time_index_file = file::open(&self.time_index_path).await?;

        let mut index_reader = BufReader::new(index_file);
        let mut time_index_reader = BufReader::new(time_index_file);

        let new_index = Vec::new();
        let mut new_index_writer = BufWriter::new(new_index);

        loop {
            let relative_offset_result = index_reader.read_u32_le().await;
            let position_result = index_reader.read_u32_le().await;

            let time_relative_offset_result = time_index_reader.read_u32_le().await;
            let timestamp_result = time_index_reader.read_u64_le().await;

            if relative_offset_result.is_err()
                || position_result.is_err()
                || time_relative_offset_result.is_err()
                || timestamp_result.is_err()
            {
                trace!("Reached EOF for index file: {}", &self.index_path);
                break;
            }

            let relative_offset = relative_offset_result?;
            let position = position_result?;
            let time_relative_offset = time_relative_offset_result?;
            let timestamp = timestamp_result?;

            if relative_offset != time_relative_offset {
                error!("{COMPONENT} - mismatched relative offsets in normal index: {relative_offset} vs time index: {time_relative_offset}");
                return Err(CompatError::IndexMigrationError);
            }

            let mut new_index_entry = BytesMut::with_capacity(INDEX_SIZE as usize); // u32 + u32 + u64
            new_index_entry.put_u32_le(relative_offset);
            new_index_entry.put_u32_le(position);
            new_index_entry.put_u64_le(timestamp);

            new_index_writer.write_all(&new_index_entry).await?;
        }
        new_index_writer.flush().await?;

        Ok(new_index_writer.into_inner())
    }

    async fn replace_with_converted(&self, indexes: Vec<u8>) -> Result<(), CompatError> {
        file::remove(&self.index_path).await?;
        file::remove(&self.time_index_path).await?;

        let dir_path = Path::new(&self.index_path).parent().ok_or({
            error!("{COMPONENT} - failed to get parent directory of index file");
            CompatError::IndexMigrationError
        })?;

        let dir = OpenOptions::new().read(true).open(dir_path).await?;

        dir.sync_all()
            .await
            .with_error(|error| {
                format!("{COMPONENT} - failed to sync data for directory, error: {error}")
            })
            .map_err(|_| CompatError::IndexMigrationError)?;

        let wait_duration = Duration::from_secs(2);
        let sleep_duration = Duration::from_millis(10);

        let wait_future = async {
            while file::exists(&self.time_index_path).await.unwrap_or(false) {
                sleep(sleep_duration).await;
            }
            info!("File {} has been removed", &self.time_index_path);
        };

        if (timeout(wait_duration, wait_future).await).is_err() {
            error!(
                "Timeout waiting for file {} to be removed",
                &self.time_index_path
            );
        }

        let new_index_file = file::overwrite(&self.index_path).await?;
        let mut new_index_writer = BufWriter::new(new_index_file);
        new_index_writer.write_all(&indexes).await?;
        new_index_writer.flush().await?;

        info!("Replaced old index with new index at {}", &self.index_path);

        Ok(())
    }
}
