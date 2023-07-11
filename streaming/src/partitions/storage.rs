use crate::partitions::partition::{ConsumerOffset, Partition};
use crate::persister::Persister;
use async_trait::async_trait;
use sdk::consumer_type::ConsumerType;
use sdk::error::Error;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::create_dir;
use tracing::{error, info, trace};

use crate::segments::segment::{Segment, LOG_EXTENSION};
use crate::storage::{PartitionStorage, Storage};

#[derive(Debug)]
pub struct FilePartitionStorage {
    persister: Arc<dyn Persister>,
}

impl FilePartitionStorage {
    pub fn new(persister: Arc<dyn Persister>) -> Self {
        Self { persister }
    }
}

unsafe impl Send for FilePartitionStorage {}
unsafe impl Sync for FilePartitionStorage {}

#[async_trait]
impl PartitionStorage for FilePartitionStorage {
    async fn save_offset(&self, offset: &ConsumerOffset) -> Result<(), Error> {
        self.persister
            .overwrite(&offset.path, &offset.offset.to_le_bytes())
            .await?;
        trace!(
            "Stored offset: {} for {}",
            offset.offset,
            offset.consumer_id,
        );
        Ok(())
    }
}

#[async_trait]
impl Storage<Partition> for FilePartitionStorage {
    async fn load(&self, partition: &mut Partition) -> Result<(), Error> {
        info!(
            "Loading partition with ID: {} for stream with ID: {} and topic with ID: {}, for path: {} from disk...",
            partition.id, partition.stream_id, partition.topic_id, partition.path
        );
        let dir_entries = fs::read_dir(&partition.path).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadPartitions(
                partition.id,
                partition.stream_id,
            ));
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
                partition.stream_id,
                partition.topic_id,
                partition.id,
                start_offset,
                &partition.path,
                partition.config.segment.clone(),
                partition.storage.clone(),
            );
            segment.load().await?;
            if !segment.is_closed {
                segment.unsaved_messages = Some(Vec::new())
            }

            // If the first segment has at least a single message, we should increment the offset.
            if !partition.should_increment_offset {
                partition.should_increment_offset = segment.current_size_bytes > 0;
            }

            if partition.config.validate_checksum {
                info!("Validating messages checksum for partition with ID: {} and segment with start offset: {}...", partition.id, segment.start_offset);
                segment.storage.segment.load_checksums(&segment).await?;
                info!("Validated messages checksum for partition with ID: {} and segment with start offset: {}.", partition.id, segment.start_offset);
            }

            // Load the unique message IDs for the partition if the deduplication feature is enabled.
            if partition.message_ids.is_some() {
                info!("Loading unique message IDs for partition with ID: {} and segment with start offset: {}...", partition.id, segment.start_offset);
                let partition_message_ids = partition.message_ids.as_mut().unwrap();
                let message_ids = segment.storage.segment.load_message_ids(&segment).await?;
                for message_id in message_ids {
                    partition_message_ids.insert(message_id, true);
                }
                info!("Loaded: {} unique message IDs for partition with ID: {} and segment with start offset: {}...", partition_message_ids.len(), partition.id, segment.start_offset);
            }

            partition.segments.push(segment);
        }

        partition
            .segments
            .sort_by(|a, b| a.start_offset.cmp(&b.start_offset));

        let end_offsets = partition
            .segments
            .iter()
            .skip(1)
            .map(|segment| segment.start_offset - 1)
            .collect::<Vec<u64>>();

        let segments_count = partition.segments.len();
        for (end_offset_index, segment) in partition.get_segments_mut().iter_mut().enumerate() {
            if end_offset_index == segments_count - 1 {
                break;
            }

            segment.end_offset = end_offsets[end_offset_index];
        }

        let last_segment = partition.segments.last_mut().unwrap();
        if last_segment.is_closed {
            last_segment.end_offset = last_segment.current_offset;
        }

        partition.current_offset = last_segment.current_offset;
        partition.load_offsets(ConsumerType::Consumer).await?;
        partition.load_offsets(ConsumerType::ConsumerGroup).await?;
        info!(
            "Loaded partition with ID: {} for stream with ID: {} and topic with ID: {}, current offset: {}.",
            partition.id, partition.stream_id, partition.topic_id, partition.current_offset
        );

        Ok(())
    }

    async fn save(&self, partition: &Partition) -> Result<(), Error> {
        info!(
            "Saving partition with start ID: {} for stream with ID: {} and topic with ID: {}...",
            partition.id, partition.stream_id, partition.topic_id
        );
        if create_dir(&partition.path).await.is_err() {
            return Err(Error::CannotCreatePartitionDirectory(
                partition.id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        if create_dir(&partition.offsets_path).await.is_err() {
            error!(
                "Failed to create offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.id, partition.stream_id, partition.topic_id
            );
            return Err(Error::CannotCreatePartition(
                partition.id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        if create_dir(&partition.consumer_offsets_path).await.is_err() {
            error!(
                "Failed to create consumer offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.id, partition.stream_id, partition.topic_id
            );
            return Err(Error::CannotCreatePartition(
                partition.id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        if create_dir(&partition.consumer_group_offsets_path)
            .await
            .is_err()
        {
            error!(
                "Failed to create consumer group offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.id, partition.stream_id, partition.topic_id
            );
            return Err(Error::CannotCreatePartition(
                partition.id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        for segment in partition.get_segments() {
            segment.persist().await?;
        }

        info!("Saved partition with start ID: {} for stream with ID: {} and topic with ID: {}, path: {}.", partition.id, partition.stream_id, partition.topic_id, partition.path);

        Ok(())
    }

    async fn delete(&self, partition: &Partition) -> Result<(), Error> {
        info!(
            "Deleting partition with ID: {} for stream with ID: {} and topic with ID: {}...",
            partition.id, partition.stream_id, partition.topic_id,
        );
        if fs::remove_dir_all(&partition.path).await.is_err() {
            return Err(Error::CannotDeletePartitionDirectory(
                partition.id,
                partition.stream_id,
                partition.topic_id,
            ));
        }
        info!(
            "Deleted partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            partition.id, partition.stream_id, partition.topic_id,
        );
        Ok(())
    }
}
