use crate::partitions::partition::Partition;
use crate::segments::log;
use crate::segments::segment::{Segment, LOG_EXTENSION};
use crate::utils::file;
use shared::error::Error;
use tokio::fs;
use tokio::fs::create_dir;
use tracing::{error, info};

impl Partition {
    pub async fn persist(&self) -> Result<(), Error> {
        info!(
            "Saving partition with start ID: {} for stream with ID: {} and topic with ID: {}...",
            self.id, self.stream_id, self.topic_id
        );
        if create_dir(&self.path).await.is_err() {
            return Err(Error::CannotCreatePartitionDirectory(
                self.id,
                self.stream_id,
                self.topic_id,
            ));
        }

        if create_dir(&self.offsets_path).await.is_err() {
            error!(
                "Failed to create offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                self.id, self.stream_id, self.topic_id
            );
            return Err(Error::CannotCreatePartition(
                self.id,
                self.stream_id,
                self.topic_id,
            ));
        }

        if create_dir(&self.consumer_offsets_path).await.is_err() {
            error!(
                "Failed to create consumer offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                self.id, self.stream_id, self.topic_id
            );
            return Err(Error::CannotCreatePartition(
                self.id,
                self.stream_id,
                self.topic_id,
            ));
        }

        for segment in self.get_segments() {
            segment.persist().await?;
        }

        info!("Saved partition with start ID: {} for stream with ID: {} and topic with ID: {}, path: {}.", self.id, self.stream_id, self.topic_id, self.path);

        Ok(())
    }

    pub async fn load(&mut self) -> Result<(), Error> {
        info!(
            "Loading partition with ID: {} for stream with ID: {} and topic with ID: {}, for path: {} from disk...",
            self.id, self.stream_id, self.topic_id, self.path
        );
        let dir_entries = fs::read_dir(&self.path).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadPartitions(self.id, self.stream_id));
        }

        let mut dir_entries = dir_entries.unwrap();
        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
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
                self.stream_id,
                self.topic_id,
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

            // Load the unique message IDs for the partition if the deduplication feature is enabled.
            if self.message_ids.is_some() {
                info!("Loading unique message IDs for partition with ID: {} and segment with start offset: {}...", self.id, segment.start_offset);
                let partition_message_ids = self.message_ids.as_mut().unwrap();
                let mut log_file = file::open_file(&segment.log_path, false).await?;
                let message_ids = log::load_message_ids(&mut log_file).await?;
                for message_id in message_ids {
                    partition_message_ids.insert(message_id, true);
                }
                info!("Loaded: {} unique message IDs for partition with ID: {} and segment with start offset: {}...", partition_message_ids.len(), self.id, segment.start_offset);
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
            "Loaded partition with ID: {} for stream with ID: {} and topic with ID: {}, current offset: {}.",
            self.id, self.stream_id, self.topic_id, self.current_offset
        );

        Ok(())
    }

    pub async fn delete(&self) -> Result<(), Error> {
        info!(
            "Deleting partition with ID: {} for stream with ID: {} and topic with ID: {}...",
            self.id, self.stream_id, self.topic_id,
        );
        if fs::remove_dir_all(&self.path).await.is_err() {
            return Err(Error::CannotDeletePartitionDirectory(
                self.id,
                self.stream_id,
                self.topic_id,
            ));
        }
        info!(
            "Deleted partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            self.id, self.stream_id, self.topic_id,
        );
        Ok(())
    }
}
