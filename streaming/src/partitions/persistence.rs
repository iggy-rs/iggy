use crate::partitions::partition::Partition;
use crate::segments::segment::{Segment, LOG_EXTENSION};
use shared::error::Error;
use tokio::fs;
use tracing::{error, info};

impl Partition {
    pub async fn persist(&self) -> Result<(), Error> {
        info!("Saving partition with start ID: {}", self.id);
        if std::fs::create_dir(&self.path).is_err() {
            return Err(Error::CannotCreatePartitionDirectory(self.id));
        }

        if std::fs::create_dir(&self.offsets_path).is_err() {
            error!(
                "Failed to create offsets directory for partition with ID: {}",
                self.id
            );
            return Err(Error::CannotCreatePartition);
        }

        if std::fs::create_dir(&self.consumer_offsets_path).is_err() {
            error!(
                "Failed to create consumer offsets directory for partition with ID: {}",
                self.id
            );
            return Err(Error::CannotCreatePartition);
        }

        for segment in self.get_segments() {
            segment.persist().await?;
        }

        Ok(())
    }

    pub async fn load(&mut self) -> Result<(), Error> {
        info!(
            "Loading partition with ID: {} from disk, for path: {}",
            self.id, self.path
        );
        let dir_files = fs::read_dir(&self.path).await;
        let mut dir_files = dir_files.unwrap();
        while let Some(dir_entry) = dir_files.next_entry().await.unwrap_or(None) {
            let metadata = dir_entry.metadata().await.unwrap();
            if metadata.is_dir() {
                continue;
            }

            let path = dir_entry.path();
            let extension = path.extension();
            if extension.is_none() || extension.unwrap() != LOG_EXTENSION {
                continue;
            }

            let log_file_name = dir_entry
                .file_name()
                .into_string()
                .unwrap()
                .replace(&format!(".{}", LOG_EXTENSION), "");

            let start_offset = log_file_name.parse::<u64>().unwrap();
            let mut segment = Segment::create(
                self.id,
                start_offset,
                &self.path,
                self.config.segment.clone(),
            );
            segment.load().await?;
            if !segment.is_closed {
                segment.unsaved_messages = Some(Vec::new())
            }

            // If the first segment has at least a single message, we should increment the offset.
            if !self.should_increment_offset {
                self.should_increment_offset = segment.current_size_bytes > 0;
            }

            self.segments.push(segment);
        }

        self.segments
            .sort_by(|a, b| a.start_offset.cmp(&b.start_offset));

        let end_offsets = self
            .segments
            .iter()
            .skip(1)
            .map(|segment| segment.start_offset - 1)
            .collect::<Vec<u64>>();

        let segments_count = self.segments.len();
        for (end_offset_index, segment) in self.get_segments_mut().iter_mut().enumerate() {
            if end_offset_index == segments_count - 1 {
                break;
            }

            segment.end_offset = end_offsets[end_offset_index];
        }

        let last_segment = self.segments.last_mut().unwrap();
        if last_segment.is_closed {
            last_segment.end_offset = last_segment.current_offset;
        }

        self.current_offset = last_segment.current_offset;
        self.load_offsets().await?;
        info!(
            "Loaded partition with ID: {}, current offset: {}",
            self.id, self.current_offset
        );

        Ok(())
    }

    pub async fn delete(&self) -> Result<(), Error> {
        info!("Deleting partition with ID: {}...", &self.id);
        if fs::remove_dir_all(&self.path).await.is_err() {
            return Err(Error::CannotDeletePartitionDirectory(self.id));
        }
        info!("Deleted partition with ID: {}.", &self.id);
        Ok(())
    }
}
