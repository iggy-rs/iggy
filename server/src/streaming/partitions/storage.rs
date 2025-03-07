use crate::compat::index_rebuilding::index_rebuilder::IndexRebuilder;
use crate::state::system::PartitionState;
use crate::streaming::batching::batch_accumulator::BatchAccumulator;
use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::partitions::COMPONENT;
use crate::streaming::persistence::persister::PersisterKind;
use crate::streaming::segments::*;
use crate::streaming::storage::PartitionStorage;
use crate::streaming::utils::file;
use error_set::ErrContext;
use iggy::consumer::ConsumerKind;
use iggy::error::IggyError;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::create_dir_all;
use tokio::io::AsyncReadExt;
use tracing::{error, info, trace, warn};

#[derive(Debug)]
pub struct FilePartitionStorage {
    persister: Arc<PersisterKind>,
}

impl FilePartitionStorage {
    pub fn new(persister: Arc<PersisterKind>) -> Self {
        Self { persister }
    }
}

impl PartitionStorage for FilePartitionStorage {
    async fn load(
        &self,
        partition: &mut Partition,
        state: PartitionState,
    ) -> Result<(), IggyError> {
        info!(
            "Loading partition with ID: {} for stream with ID: {} and topic with ID: {}, for path: {} from disk...",
            partition.partition_id, partition.stream_id, partition.topic_id, partition.partition_path
        );
        partition.created_at = state.created_at;
        let dir_entries = fs::read_dir(&partition.partition_path).await;
        if fs::read_dir(&partition.partition_path)
                .await
                .inspect_err(|err| {
                    error!(
                            "Failed to read partition with ID: {} for stream with ID: {} and topic with ID: {} and path: {}. Error: {}",
                            partition.partition_id, partition.stream_id, partition.topic_id, partition.partition_path, err
                        );
                })
                .with_error_context(|error| format!(
                    "{COMPONENT} (error: {error}) - failed to read partition with ID: {} for stream with ID: {} and topic with ID: {} and path: {}.",
                    partition.partition_id, partition.stream_id, partition.topic_id, partition.partition_path,
                )).is_err()
            {
                return Err(IggyError::CannotReadPartitions);
            }

        let mut dir_entries = dir_entries.unwrap();
        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
            let path = dir_entry.path();
            let extension = path.extension();
            if extension.is_none() || extension.unwrap() != LOG_EXTENSION {
                continue;
            }
            let metadata = dir_entry.metadata().await.unwrap();
            if metadata.is_dir() {
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
                partition.message_expiry,
                partition.size_of_parent_stream.clone(),
                partition.size_of_parent_topic.clone(),
                partition.size_bytes.clone(),
                partition.messages_count_of_parent_stream.clone(),
                partition.messages_count_of_parent_topic.clone(),
                partition.messages_count.clone(),
            );

            let index_path = segment.index_path.to_owned();
            let log_path = segment.log_path.to_owned();
            let time_index_path = index_path.replace(INDEX_EXTENSION, "timeindex");

            let index_cache_enabled = partition.config.segment.cache_indexes;

            let index_path_exists = tokio::fs::try_exists(&index_path).await.unwrap();
            let time_index_path_exists = tokio::fs::try_exists(&time_index_path).await.unwrap();

            // Rebuild indexes in 2 cases:
            // 1. Index cache is enabled and index at path does not exists.
            // 2. Index cache is enabled and time index at path exists.
            if index_cache_enabled && (!index_path_exists || time_index_path_exists) {
                warn!(
                    "Index at path {} does not exist, rebuilding it based on {}...",
                    index_path, log_path
                );
                let now = tokio::time::Instant::now();
                let index_rebuilder =
                    IndexRebuilder::new(log_path.clone(), index_path.clone(), start_offset);
                index_rebuilder.rebuild().await.unwrap_or_else(|e| {
                    panic!(
                        "Failed to rebuild index for partition with ID: {} for
                    stream with ID: {} and topic with ID: {}. Error: {e}",
                        partition.partition_id, partition.stream_id, partition.topic_id,
                    )
                });
                info!(
                    "Rebuilding index for path {} finished, it took {} ms",
                    index_path,
                    now.elapsed().as_millis()
                );
            }

            // Remove legacy time index if it exists.
            if time_index_path_exists {
                tokio::fs::remove_file(&time_index_path).await.unwrap();
            }

            segment.load_from_disk().await.with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to load segment: {segment}",)
            })?;
            let capacity = partition.config.partition.messages_required_to_save;
            if !segment.is_closed {
                segment.unsaved_messages = Some(Default::default());
            }

            // If the first segment has at least a single message, we should increment the offset.
            if !partition.should_increment_offset {
                partition.should_increment_offset = segment.size_bytes > 0;
            }

            if partition.config.partition.validate_checksum {
                info!("Validating messages checksum for partition with ID: {} and segment with start offset: {}...", partition.partition_id, segment.start_offset);
                segment.load_message_checksums().await?;
                info!("Validated messages checksum for partition with ID: {} and segment with start offset: {}.", partition.partition_id, segment.start_offset);
            }

            // Load the unique message IDs for the partition if the deduplication feature is enabled.
            let mut unique_message_ids_count = 0;
            if let Some(message_deduplicator) = &partition.message_deduplicator {
                info!("Loading unique message IDs for partition with ID: {} and segment with start offset: {}...", partition.partition_id, segment.start_offset);
                let message_ids = segment.load_message_ids().await.with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to load message ids, segment: {segment}",)
                })?;
                for message_id in message_ids {
                    if message_deduplicator.try_insert(&message_id).await {
                        unique_message_ids_count += 1;
                    } else {
                        warn!("Duplicated message ID: {} for partition with ID: {} and segment with start offset: {}.", message_id, partition.partition_id, segment.start_offset);
                    }
                }
                info!("Loaded: {} unique message IDs for partition with ID: {} and segment with start offset: {}...", unique_message_ids_count, partition.partition_id, segment.start_offset);
            }

            partition
                .segments_count_of_parent_stream
                .fetch_add(1, Ordering::SeqCst);
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

        partition
            .load_consumer_offsets()
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to load consumer offsets, partition: {partition}",)
            })?;
        info!(
            "Loaded partition with ID: {} for stream with ID: {} and topic with ID: {}, current offset: {}.",
            partition.partition_id, partition.stream_id, partition.topic_id, partition.current_offset
        );

        Ok(())
    }

    async fn save(&self, partition: &mut Partition) -> Result<(), IggyError> {
        info!(
            "Saving partition with start ID: {} for stream with ID: {} and topic with ID: {}...",
            partition.partition_id, partition.stream_id, partition.topic_id
        );
        if !Path::new(&partition.partition_path).exists()
            && create_dir_all(&partition.partition_path).await.is_err()
        {
            return Err(IggyError::CannotCreatePartitionDirectory(
                partition.partition_id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        if !Path::new(&partition.offsets_path).exists()
            && create_dir_all(&partition.offsets_path).await.is_err()
        {
            error!(
                "Failed to create offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.partition_id, partition.stream_id, partition.topic_id
            );
            return Err(IggyError::CannotCreatePartition(
                partition.partition_id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        if !Path::new(&partition.consumer_offsets_path).exists()
            && create_dir_all(&partition.consumer_offsets_path)
                .await
                .is_err()
        {
            error!(
                "Failed to create consumer offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.partition_id, partition.stream_id, partition.topic_id
            );
            return Err(IggyError::CannotCreatePartition(
                partition.partition_id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        if !Path::new(&partition.consumer_group_offsets_path).exists()
            && create_dir_all(&partition.consumer_group_offsets_path)
                .await
                .is_err()
        {
            error!(
                "Failed to create consumer group offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                partition.partition_id, partition.stream_id, partition.topic_id
            );
            return Err(IggyError::CannotCreatePartition(
                partition.partition_id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        for segment in partition.get_segments_mut() {
            segment.persist().await.with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to persist segment: {segment}",)
            })?;
        }

        info!("Saved partition with start ID: {} for stream with ID: {} and topic with ID: {}, path: {}.", partition.partition_id, partition.stream_id, partition.topic_id, partition.partition_path);

        Ok(())
    }

    async fn delete(&self, partition: &Partition) -> Result<(), IggyError> {
        info!(
            "Deleting partition with ID: {} for stream with ID: {} and topic with ID: {}...",
            partition.partition_id, partition.stream_id, partition.topic_id,
        );

        if let Err(err) = self
            .delete_consumer_offsets(&partition.consumer_offsets_path)
            .await
        {
            error!("Cannot delete consumer offsets for partition with ID: {} for topic with ID: {} for stream with ID: {}. Error: {}", partition.partition_id, partition.topic_id, partition.stream_id, err);
            return Err(IggyError::CannotDeletePartition(
                partition.partition_id,
                partition.topic_id,
                partition.stream_id,
            ));
        }

        if let Err(err) = self
            .delete_consumer_offsets(&partition.consumer_group_offsets_path)
            .await
        {
            error!("Cannot delete consumer group offsets for partition with ID: {} for topic with ID: {} for stream with ID: {}. Error: {}", partition.partition_id, partition.topic_id, partition.stream_id, err);
            return Err(IggyError::CannotDeletePartition(
                partition.partition_id,
                partition.topic_id,
                partition.stream_id,
            ));
        }

        if fs::remove_dir_all(&partition.partition_path).await.is_err() {
            error!("Cannot delete partition directory: {} for partition with ID: {} for topic with ID: {} for stream with ID: {}.", partition.partition_path, partition.partition_id, partition.topic_id, partition.stream_id);
            return Err(IggyError::CannotDeletePartitionDirectory(
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

    async fn save_consumer_offset(&self, offset: u64, path: &str) -> Result<(), IggyError> {
        self.persister
            .overwrite(path, &offset.to_le_bytes())
            .await
            .with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - failed to overwrite consumer offset with value: {}, path: {}",
                offset, path,
            ))?;
        trace!("Stored consumer offset value: {}, path: {}", offset, path);
        Ok(())
    }

    async fn load_consumer_offsets(
        &self,
        kind: ConsumerKind,
        path: &str,
    ) -> Result<Vec<ConsumerOffset>, IggyError> {
        trace!("Loading consumer offsets from path: {path}...");
        let dir_entries = fs::read_dir(&path).await;
        if dir_entries.is_err() {
            return Err(IggyError::CannotReadConsumerOffsets(path.to_owned()));
        }

        let mut consumer_offsets = Vec::new();
        let mut dir_entries = dir_entries.unwrap();
        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
            let metadata = dir_entry.metadata().await;
            if metadata.is_err() {
                break;
            }

            if metadata.unwrap().is_dir() {
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

            let path = Arc::new(path.unwrap().to_string());
            let consumer_id = consumer_id.unwrap();
            let mut file = file::open(&path)
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to open offset file, path: {path}"
                    )
                })
                .map_err(|_| IggyError::CannotReadFile)?;
            let offset = file
                .read_u64_le()
                .await
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to read consumer offset from file, path: {path}")
                })
                .map_err(|_| IggyError::CannotReadFile)?;

            consumer_offsets.push(ConsumerOffset {
                kind,
                consumer_id,
                offset,
                path,
            });
        }

        consumer_offsets.sort_by(|a, b| a.consumer_id.cmp(&b.consumer_id));
        Ok(consumer_offsets)
    }

    async fn delete_consumer_offsets(&self, path: &str) -> Result<(), IggyError> {
        if !Path::new(path).exists() {
            trace!("Consumer offsets directory does not exist: {path}.");
            return Ok(());
        }

        if fs::remove_dir_all(path).await.is_err() {
            error!("Cannot delete consumer offsets directory: {}.", path);
            return Err(IggyError::CannotDeleteConsumerOffsetsDirectory(
                path.to_owned(),
            ));
        }
        Ok(())
    }

    async fn delete_consumer_offset(&self, path: &str) -> Result<(), IggyError> {
        if !Path::new(path).exists() {
            trace!("Consumer offset file does not exist: {path}.");
            return Ok(());
        }

        if fs::remove_file(path).await.is_err() {
            error!("Cannot delete consumer offset file: {path}.");
            return Err(IggyError::CannotDeleteConsumerOffsetFile(path.to_owned()));
        }
        Ok(())
    }
}
