use crate::compat::message_conversion::message_converter::MessageFormatConverter;
use crate::state::system::PartitionState;
use crate::streaming::batching::batch_accumulator::BatchAccumulator;
use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::persistence::persister::Persister;
use crate::streaming::segments::segment::{Segment, LOG_EXTENSION};
use crate::streaming::storage::PartitionStorage;
use crate::streaming::utils::file;
use anyhow::Context;
use async_trait::async_trait;
use iggy::consumer::ConsumerKind;
use iggy::error::IggyError;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::create_dir;
use tokio::io::AsyncReadExt;
use tracing::{error, info, trace, warn};

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
        if let Err(err) = fs::read_dir(&partition.partition_path)
                .await
                .with_context(|| format!("Failed to read partition with ID: {} for stream with ID: {} and topic with ID: {} and path: {}", partition.partition_id, partition.stream_id, partition.topic_id, partition.partition_path))
            {
                return Err(IggyError::CannotReadPartitions(err));
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
                partition.partition_id,
                start_offset,
                partition.config.clone(),
                partition.storage.clone(),
                partition.message_expiry,
                partition.size_of_parent_stream.clone(),
                partition.size_of_parent_topic.clone(),
                partition.size_bytes.clone(),
                partition.messages_count_of_parent_stream.clone(),
                partition.messages_count_of_parent_topic.clone(),
                partition.messages_count.clone(),
            );

            let log_path = segment.log_path.to_owned();
            let index_path = segment.index_path.to_owned();
            let message_format_converter =
                MessageFormatConverter::init(start_offset, log_path, index_path);

            info!("Attempting to detect changes in binary schema for partition with ID: {} and segment with start offset: {}", partition.partition_id, start_offset);
            let samplers_count = message_format_converter.samplers.len();
            // Check if partition has any segments
            for (idx, sampler) in message_format_converter.samplers.iter().enumerate() {
                trace!("Trying to sample the message format for partition with ID: {} and segment with start offset: {}", partition.partition_id, start_offset);
                match sampler.try_sample().await {
                    Ok(schema) if idx == 0 => {
                        // Found message in the newest format, no conversion needed
                        trace!("Detected up to date binary schema: {:?}, for partition with ID: {} and segment with start offset: {}", schema, partition.partition_id, start_offset);
                        break;
                    }
                    Ok(schema) => {
                        // Found old format, need to convert it
                        info!("Detected changes in binary schema for partition with ID: {} and segment with start offset: {}", partition.partition_id, start_offset);
                        segment.convert_segment_from_schema(schema).await?;
                    }
                    Err(err) if idx + 1 == samplers_count => {
                        // Didn't find any message format, return an error
                        return Err(IggyError::CannotLoadResource(anyhow::anyhow!(err)
                                .context(format!(
                                    "Failed to find a valid message format, when trying to perform a conversion for partition with ID: {} and segment with start offset: {}.",
                                    partition.partition_id,
                                    start_offset
                                ))));
                    }
                    _ => {}
                }
            }

            segment.load().await?;
            let capacity = partition.config.partition.messages_required_to_save;
            if !segment.is_closed {
                segment.unsaved_messages = Some(BatchAccumulator::new(
                    segment.current_offset,
                    capacity as usize,
                ))
            }

            // If the first segment has at least a single message, we should increment the offset.
            if !partition.should_increment_offset {
                partition.should_increment_offset = segment.size_bytes > 0;
            }

            if partition.config.partition.validate_checksum {
                info!("Validating messages checksum for partition with ID: {} and segment with start offset: {}...", partition.partition_id, segment.start_offset);
                segment.storage.segment.load_checksums(&segment).await?;
                info!("Validated messages checksum for partition with ID: {} and segment with start offset: {}.", partition.partition_id, segment.start_offset);
            }

            // Load the unique message IDs for the partition if the deduplication feature is enabled.
            let mut unique_message_ids_count = 0;
            if let Some(message_deduplicator) = &partition.message_deduplicator {
                info!("Loading unique message IDs for partition with ID: {} and segment with start offset: {}...", partition.partition_id, segment.start_offset);
                let message_ids = segment.storage.segment.load_message_ids(&segment).await?;
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

        partition.load_consumer_offsets().await?;
        info!(
            "Loaded partition with ID: {} for stream with ID: {} and topic with ID: {}, current offset: {}.",
            partition.partition_id, partition.stream_id, partition.topic_id, partition.current_offset
        );

        Ok(())
    }

    async fn save(&self, partition: &Partition) -> Result<(), IggyError> {
        info!(
            "Saving partition with start ID: {} for stream with ID: {} and topic with ID: {}...",
            partition.partition_id, partition.stream_id, partition.topic_id
        );
        if !Path::new(&partition.partition_path).exists()
            && create_dir(&partition.partition_path).await.is_err()
        {
            return Err(IggyError::CannotCreatePartitionDirectory(
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
            return Err(IggyError::CannotCreatePartition(
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
            return Err(IggyError::CannotCreatePartition(
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
            return Err(IggyError::CannotCreatePartition(
                partition.partition_id,
                partition.stream_id,
                partition.topic_id,
            ));
        }

        for segment in partition.get_segments() {
            segment.persist().await?;
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

    async fn save_consumer_offset(&self, offset: &ConsumerOffset) -> Result<(), IggyError> {
        self.persister
            .overwrite(&offset.path, &offset.offset.to_le_bytes())
            .await?;
        trace!(
            "Stored consumer offset value: {} for {} with ID: {}, path: {}",
            offset.offset,
            offset.kind,
            offset.consumer_id,
            offset.path
        );
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

            let path = path.unwrap().to_string();
            let consumer_id = consumer_id.unwrap();
            let mut file = file::open(&path).await?;
            let offset = file.read_u64_le().await?;

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
