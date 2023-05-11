use crate::segments::segment::Segment;
use crate::segments::time_index;
use crate::utils::file;
use shared::error::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::info;

impl Segment {
    pub async fn load(&mut self) -> Result<(), Error> {
        info!(
            "Loading segment from disk for start offset: {} and partition with ID: {}...",
            self.start_offset, self.partition_id
        );
        let log_file = file::open_file(&self.log_path, false).await;
        let file_size = log_file.metadata().await.unwrap().len() as u32;
        self.current_size_bytes = file_size;
        self.saved_bytes = self.current_size_bytes;

        info!(
            "Segment log file for start offset {}, current offset: {}, and partition ID: {} has {} bytes of size.",
            self.start_offset, self.current_offset, self.partition_id, self.current_size_bytes
        );

        let mut time_index_file = file::open_file(&self.time_index_path, false).await;

        info!(
            "Loading time indexes for segment with start offset: {} and partition ID: {}...",
            self.start_offset, self.partition_id
        );

        self.time_indexes = time_index::load(&mut time_index_file).await?;

        if !self.time_indexes.is_empty() {
            let last_index = self.time_indexes.last().unwrap();
            self.current_offset = self.start_offset + last_index.relative_offset as u64;
        }

        if self.is_full() {
            self.is_closed = true;
        }

        Ok(())
    }

    pub async fn persist(&mut self) -> Result<(), Error> {
        info!("Saving segment with start offset: {}", self.start_offset);
        if File::create(&self.log_path).await.is_err() {
            return Err(Error::CannotCreatePartitionSegmentLogFile(
                self.log_path.clone(),
            ));
        }

        if File::create(&self.time_index_path).await.is_err() {
            return Err(Error::CannotCreatePartitionSegmentTimeIndexFile(
                self.log_path.clone(),
            ));
        }

        let index_file = File::create(&self.index_path).await;
        if index_file.is_err() {
            return Err(Error::CannotCreatePartitionSegmentIndexFile(
                self.log_path.clone(),
            ));
        }

        let mut index_file = index_file.unwrap();
        let zero_index = 0u64.to_le_bytes();
        if index_file.write_all(&zero_index).await.is_err() {
            return Err(Error::CannotSaveIndexToSegment);
        }

        info!(
            "Created partition segment log file for start offset: {} and partition with ID: {} and path: {}.",
            self.start_offset, self.partition_id, self.log_path
        );

        Ok(())
    }
}
