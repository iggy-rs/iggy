use crate::partitions::partition::{ConsumerOffset, Partition};
use crate::persister::Persister;
use async_trait::async_trait;
use iggy::consumer::ConsumerKind;
use iggy::error::Error;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::create_dir;
use tracing::{error, info, trace};

use crate::segments::segment::{Segment, LOG_EXTENSION};
use crate::storage::{PartitionStorage, Storage};

#[derive(Debug)]
pub struct FilePartitionStorage {
    db: Arc<Db>,
    persister: Arc<dyn Persister>,
}

impl FilePartitionStorage {
    pub fn new(db: Arc<Db>, persister: Arc<dyn Persister>) -> Self {
        Self { db, persister }
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

#[derive(Debug, Serialize, Deserialize)]
struct PartitionData {
    created_at: u64,
}

#[async_trait]
impl Storage<Partition> for FilePartitionStorage {
    async fn load(&self, partition: &mut Partition) -> Result<(), Error> {
        info!(
            "Loading partition with ID: {} for stream with ID: {} and topic with ID: {}, for path: {} from disk...",
            partition.partition_id, partition.stream_id, partition.topic_id, partition.path
        );
        let dir_entries = fs::read_dir(&partition.path).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadPartitions(
                partition.partition_id,
                partition.stream_id,
            ));
        }

        let key = get_key(
            partition.stream_id,
            partition.topic_id,
            partition.partition_id,
        );
        let partition_data = self.db.get(&key);
        if partition_data.is_err() {
            return Err(Error::CannotLoadResource(key));
        }

        let partition_data = partition_data.unwrap();
        if partition_data.is_none() {
            return Err(Error::ResourceNotFound(key));
        }

        let partition_data = partition_data.unwrap();
        let partition_data = rmp_serde::from_slice::<PartitionData>(&partition_data);
        if partition_data.is_err() {
            return Err(Error::CannotDeserializeResource(key));
        }

        let partition_data = partition_data.unwrap();
        partition.created_at = partition_data.created_at;

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
                partition.partition_id,
                start_offset,
                partition.config.clone(),
                partition.storage.clone(),
                partition.message_expiry,
            );
            segment.load().await?;
            if !segment.is_closed {
                segment.unsaved_messages = Some(Vec::new())
            }

            // If the first segment has at least a single message, we should increment the offset.
            if !partition.should_increment_offset {
                partition.should_increment_offset = segment.current_size_bytes > 0;
            }

            if partition.config.partition.validate_checksum {
                info!("Validating messages checksum for partition with ID: {} and segment with start offset: {}...", partition.partition_id, segment.start_offset);
                segment.storage.segment.load_checksums(&segment).await?;
                info!("Validated messages checksum for partition with ID: {} and segment with start offset: {}.", partition.partition_id, segment.start_offset);
            }

            // Load the unique message IDs for the partition if the deduplication feature is enabled.
            if partition.message_ids.is_some() {
                info!("Loading unique message IDs for partition with ID: {} and segment with start offset: {}...", partition.partition_id, segment.start_offset);
                let partition_message_ids = partition.message_ids.as_mut().unwrap();
                let message_ids = segment.storage.segment.load_message_ids(&segment).await?;
                for message_id in message_ids {
                    partition_message_ids.insert(message_id, true);
                }
                info!("Loaded: {} unique message IDs for partition with ID: {} and segment with start offset: {}...", partition_message_ids.len(), partition.partition_id, segment.start_offset);
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

        if !partition.segments.is_empty() {
            let last_segment = partition.segments.last_mut().unwrap();
            if last_segment.is_closed {
                last_segment.end_offset = last_segment.current_offset;
            }

            partition.current_offset = last_segment.current_offset;
        }

        partition.load_offsets(ConsumerKind::Consumer).await?;
        partition.load_offsets(ConsumerKind::ConsumerGroup).await?;
        info!(
            "Loaded partition with ID: {} for stream with ID: {} and topic with ID: {}, current offset: {}.",
            partition.partition_id, partition.stream_id, partition.topic_id, partition.current_offset
        );

        Ok(())
    }

    async fn save(&self, partition: &Partition) -> Result<(), Error> {
        info!(
            "Saving partition with start ID: {} for stream with ID: {} and topic with ID: {}...",
            partition.partition_id, partition.stream_id, partition.topic_id
        );
        if !Path::new(&partition.path).exists() && create_dir(&partition.path).await.is_err() {
            return Err(Error::CannotCreatePartitionDirectory(
                partition.partition_id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        if !Path::new(&partition.offsets_path).exists()
            && create_dir(&partition.offsets_path).await.is_err()
        {
            error!(
                "Failed to create offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.partition_id, partition.stream_id, partition.topic_id
            );
            return Err(Error::CannotCreatePartition(
                partition.partition_id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        if !Path::new(&partition.consumer_offsets_path).exists()
            && create_dir(&partition.consumer_offsets_path).await.is_err()
        {
            error!(
                "Failed to create consumer offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.partition_id, partition.stream_id, partition.topic_id
            );
            return Err(Error::CannotCreatePartition(
                partition.partition_id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        if !Path::new(&partition.consumer_group_offsets_path).exists()
            && create_dir(&partition.consumer_group_offsets_path)
                .await
                .is_err()
        {
            error!(
                "Failed to create consumer group offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.partition_id, partition.stream_id, partition.topic_id
            );
            return Err(Error::CannotCreatePartition(
                partition.partition_id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        let key = get_key(
            partition.stream_id,
            partition.topic_id,
            partition.partition_id,
        );
        match rmp_serde::to_vec(&PartitionData {
            created_at: partition.created_at,
        }) {
            Ok(data) => {
                if let Err(err) = self.db.insert(&key, data) {
                    error!("Cannot save partition with ID: {} for topic with ID: {} for stream with ID: {}. Error: {}", partition.partition_id, partition.topic_id, partition.stream_id, err);
                    return Err(Error::CannotSaveResource(key.to_string()));
                }
            }
            Err(err) => {
                error!("Cannot serialize partition with ID: {} for topic with ID: {} for stream with ID: {}. Error: {}", partition.partition_id, partition.topic_id, partition.stream_id, err);
                return Err(Error::CannotSerializeResource(key));
            }
        }

        if self
            .db
            .insert(
                &key,
                rmp_serde::to_vec(&PartitionData {
                    created_at: partition.created_at,
                })
                .unwrap(),
            )
            .is_err()
        {
            return Err(Error::CannotSaveResource(key));
        }

        for segment in partition.get_segments() {
            segment.persist().await?;
        }

        info!("Saved partition with start ID: {} for stream with ID: {} and topic with ID: {}, path: {}.", partition.partition_id, partition.stream_id, partition.topic_id, partition.path);

        Ok(())
    }

    async fn delete(&self, partition: &Partition) -> Result<(), Error> {
        info!(
            "Deleting partition with ID: {} for stream with ID: {} and topic with ID: {}...",
            partition.partition_id, partition.stream_id, partition.topic_id,
        );
        if self
            .db
            .remove(get_key(
                partition.stream_id,
                partition.topic_id,
                partition.partition_id,
            ))
            .is_err()
        {
            return Err(Error::CannotDeletePartition(
                partition.partition_id,
                partition.topic_id,
                partition.stream_id,
            ));
        }

        if fs::remove_dir_all(&partition.path).await.is_err() {
            return Err(Error::CannotDeletePartitionDirectory(
                partition.partition_id,
                partition.stream_id,
                partition.topic_id,
            ));
        }
        info!(
            "Deleted partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            partition.partition_id, partition.stream_id, partition.topic_id,
        );
        Ok(())
    }
}

fn get_key(stream_id: u32, topic_id: u32, partition_id: u32) -> String {
    format!(
        "streams:{}:topics:{}:partitions:{}",
        stream_id, topic_id, partition_id
    )
}
