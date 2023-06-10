use crate::segments::segment::Segment;
use crate::segments::{index, time_index};
use crate::utils::file;
use shared::error::Error;
use tokio::fs::File;
use tracing::info;

impl Segment {
    pub async fn load(&mut self) -> Result<(), Error> {
        info!(
            "Loading segment from disk for start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {} ...",
            self.start_offset, self.partition_id, self.topic_id, self.stream_id
        );
        let log_file = file::open_file(&self.log_path, false).await?;
        let file_size = log_file.metadata().await.unwrap().len() as u32;
        self.current_size_bytes = file_size;

        info!(
            "Segment log file for start offset {}, current offset: {}, and partition with ID: {} for topic with ID: {} and stream with ID: {} has {} bytes of size.",
            self.start_offset, self.current_offset, self.partition_id, self.topic_id, self.stream_id, self.current_size_bytes
        );

        let mut index_file = file::open_file(&self.index_path, false).await?;
        let mut time_index_file = file::open_file(&self.time_index_path, false).await?;

        if self.config.cache_indexes {
            self.indexes = Some(index::load_all(&mut index_file).await?);
            info!(
                "Loaded {} indexes for segment with start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                self.indexes.as_ref().unwrap().len(),
                self.start_offset,
                self.partition_id,
                self.topic_id,
                self.stream_id
            );
        }

        if self.config.cache_time_indexes {
            let time_indexes = time_index::load_all(&mut time_index_file).await?;
            if !time_indexes.is_empty() {
                let last_index = time_indexes.last().unwrap();
                self.current_offset = self.start_offset + last_index.relative_offset as u64;
                self.time_indexes = Some(time_indexes);
            }

            info!(
                "Loaded {} time indexes for segment with start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                self.time_indexes.as_ref().unwrap().len(),
                self.start_offset,
                self.partition_id,
                self.topic_id,
                self.stream_id
            );
        } else {
            let last_index = time_index::load_last(&mut time_index_file).await?;
            if let Some(last_index) = last_index {
                self.current_offset = self.start_offset + last_index.relative_offset as u64;
                info!(
                "Loaded last time index for segment with start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                self.start_offset,
                self.partition_id,
                self.topic_id,
                self.stream_id
            );
            }
        }

        if self.is_full() {
            self.is_closed = true;
        }

        Ok(())
    }

    pub async fn persist(&self) -> Result<(), Error> {
        info!("Saving segment with start offset: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}",
            self.start_offset, self.partition_id, self.topic_id, self.stream_id);
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

        info!("Created segment log file with start offset: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}",
            self.start_offset, self.partition_id, self.topic_id, self.stream_id);

        Ok(())
    }
}
