use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::segments::segment::{Segment, LOG_EXTENSION};
use crate::streaming::storage::{PartitionStorage, Storage};
use anyhow::Context;
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

#[derive(Debug)]
pub struct FilePartitionStorage {
    db: Arc<Db>,
}

impl FilePartitionStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

unsafe impl Send for FilePartitionStorage {}
unsafe impl Sync for FilePartitionStorage {}

#[async_trait]
impl PartitionStorage for FilePartitionStorage {
    async fn save_consumer_offset(&self, offset: &ConsumerOffset) -> Result<(), Error> {
        // The stored value is just the offset, so we don't need to serialize the whole struct.
        // It should be as fast and lightweight as possible.
        // As described in the docs, sled works better with big-endian byte order.
        if let Err(err) = self
            .db
            .insert(&offset.key, &offset.offset.to_be_bytes())
            .with_context(|| {
                format!(
                    "Failed to save consumer offset: {}, key: {}",
                    offset.offset, offset.key
                )
            })
        {
            return Err(Error::CannotSaveResource(err));
        }

        trace!(
            "Stored consumer offset value: {} for {} with ID: {}",
            offset.offset,
            offset.kind,
            offset.consumer_id
        );
        Ok(())
    }

    async fn load_consumer_offsets(
        &self,
        kind: ConsumerKind,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
    ) -> Result<Vec<ConsumerOffset>, Error> {
        let mut consumer_offsets = Vec::new();
        let key_prefix = format!(
            "{}:",
            ConsumerOffset::get_key_prefix(kind, stream_id, topic_id, partition_id)
        );
        for data in self.db.scan_prefix(&key_prefix) {
            let consumer_offset = match data.with_context(|| {
                format!(
                    "Failed to load consumer offset, when searching by key: {}",
                    key_prefix
                )
            }) {
                Ok((key, value)) => {
                    let key = String::from_utf8(key.to_vec()).unwrap();
                    let offset = u64::from_be_bytes(value.as_ref().try_into().unwrap());
                    let consumer_id = key.split(':').last().unwrap().parse::<u32>().unwrap();
                    ConsumerOffset {
                        key,
                        kind,
                        consumer_id,
                        offset,
                    }
                }
                Err(err) => {
                    return Err(Error::CannotLoadResource(err));
                }
            };
            consumer_offsets.push(consumer_offset);
        }

        consumer_offsets.sort_by(|a, b| a.consumer_id.cmp(&b.consumer_id));
        Ok(consumer_offsets)
    }

    async fn delete_consumer_offsets(
        &self,
        kind: ConsumerKind,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
    ) -> Result<(), Error> {
        let consumer_offset_key_prefix = format!(
            "{}:",
            ConsumerOffset::get_key_prefix(kind, stream_id, topic_id, partition_id)
        );

        for data in self.db.scan_prefix(&consumer_offset_key_prefix) {
            match data.with_context(|| {
                format!(
                    "Failed to delete consumer offset, when searching by key: {}",
                    consumer_offset_key_prefix
                )
            }) {
                Ok((key, _)) => {
                    if let Err(err) = self.db.remove(&key).with_context(|| {
                        format!("Failed to delete consumer offset, key: {:?}", key)
                    }) {
                        return Err(Error::CannotLoadResource(err));
                    }
                }
                Err(err) => {
                    return Err(Error::CannotLoadResource(err));
                }
            }
        }

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
        if let Err(err) = fs::read_dir(&partition.path)
            .await
            .with_context(|| format!("Failed to read partition with ID: {} for stream with ID: {} and topic with ID: {} and path: {}", partition.partition_id, partition.stream_id, partition.topic_id, partition.path))
        {
            return Err(Error::CannotReadPartitions(err));
        }

        let key = get_partition_key(
            partition.stream_id,
            partition.topic_id,
            partition.partition_id,
        );
        let partition_data = match self
            .db
            .get(&key)
            .with_context(|| format!("Failed to load partition with key: {}", key))
        {
            Ok(partition_data) => {
                if let Some(partition_data) = partition_data {
                    let partition_data = rmp_serde::from_slice::<PartitionData>(&partition_data)
                        .with_context(|| {
                            format!("Failed to deserialize partition with key: {}", key)
                        });
                    if let Err(err) = partition_data {
                        return Err(Error::CannotDeserializeResource(err));
                    } else {
                        partition_data.unwrap()
                    }
                } else {
                    return Err(Error::ResourceNotFound(key));
                }
            }
            Err(err) => {
                return Err(Error::CannotLoadResource(err));
            }
        };

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
                    partition_message_ids.insert(message_id);
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

        partition.load_consumer_offsets().await?;
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

        let key = get_partition_key(
            partition.stream_id,
            partition.topic_id,
            partition.partition_id,
        );
        match rmp_serde::to_vec(&PartitionData {
            created_at: partition.created_at,
        })
        .with_context(|| format!("Failed to serialize partition with key: {}", key))
        {
            Ok(data) => {
                if let Err(err) = self
                    .db
                    .insert(&key, data)
                    .with_context(|| format!("Failed to insert partition with key: {}", key))
                {
                    return Err(Error::CannotSaveResource(err));
                }
            }
            Err(err) => {
                return Err(Error::CannotSerializeResource(err));
            }
        }

        if let Err(err) = self
            .db
            .insert(
                &key,
                rmp_serde::to_vec(&PartitionData {
                    created_at: partition.created_at,
                })
                .unwrap(),
            )
            .with_context(|| format!("Failed to insert partition with key: {}", key))
        {
            return Err(Error::CannotSaveResource(err));
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
            .remove(get_partition_key(
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

        if let Err(err) = self
            .delete_consumer_offsets(
                ConsumerKind::Consumer,
                partition.stream_id,
                partition.topic_id,
                partition.partition_id,
            )
            .await
        {
            error!("Cannot delete consumer offsets for partition with ID: {} for topic with ID: {} for stream with ID: {}. Error: {}", partition.partition_id, partition.topic_id, partition.stream_id, err);
            return Err(Error::CannotDeletePartition(
                partition.partition_id,
                partition.topic_id,
                partition.stream_id,
            ));
        }

        if let Err(err) = self
            .delete_consumer_offsets(
                ConsumerKind::ConsumerGroup,
                partition.stream_id,
                partition.topic_id,
                partition.partition_id,
            )
            .await
        {
            error!("Cannot delete consumer group offsets for partition with ID: {} for topic with ID: {} for stream with ID: {}. Error: {}", partition.partition_id, partition.topic_id, partition.stream_id, err);
            return Err(Error::CannotDeletePartition(
                partition.partition_id,
                partition.topic_id,
                partition.stream_id,
            ));
        }

        if fs::remove_dir_all(&partition.path).await.is_err() {
            error!("Cannot delete partition directory: {} for partition with ID: {} for topic with ID: {} for stream with ID: {}.", partition.path, partition.partition_id, partition.topic_id, partition.stream_id);
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

fn get_partition_key(stream_id: u32, topic_id: u32, partition_id: u32) -> String {
    format!(
        "streams:{}:topics:{}:partitions:{}",
        stream_id, topic_id, partition_id
    )
}
