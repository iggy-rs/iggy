use crate::configs::system::SystemConfig;
use crate::streaming::batching::batch_accumulator::BatchAccumulator;
use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::segments::segment::{Segment, LOG_EXTENSION};
use anyhow::Context;
use iggy::consumer::ConsumerKind;
use iggy::error::IggyError;
use iggy::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::sync::atomic::Ordering;
use tokio::fs;
use tracing::{info, warn};

pub async fn load_consumer_offsets(
    db: &Db,
    config: &SystemConfig,
    kind: ConsumerKind,
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
) -> Result<Vec<ConsumerOffset>, IggyError> {
    let mut consumer_offsets = Vec::new();
    let key_prefix = format!(
        "{}:",
        get_key_prefix(kind, stream_id, topic_id, partition_id)
    );
    for data in db.scan_prefix(&key_prefix) {
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
                ConsumerOffsetCompat {
                    key,
                    kind,
                    consumer_id,
                    offset,
                }
            }
            Err(err) => {
                return Err(IggyError::CannotLoadResource(err));
            }
        };
        consumer_offsets.push(consumer_offset);
    }

    consumer_offsets.sort_by(|a, b| a.consumer_id.cmp(&b.consumer_id));
    let consumer_offsets = consumer_offsets
        .into_iter()
        .map(|consumer_offset| {
            let path = match kind {
                ConsumerKind::Consumer => {
                    config.get_consumer_offsets_path(stream_id, topic_id, partition_id)
                }
                ConsumerKind::ConsumerGroup => {
                    config.get_consumer_group_offsets_path(stream_id, topic_id, partition_id)
                }
            };
            let path = format!("{path}/{}", consumer_offset.consumer_id);
            ConsumerOffset {
                kind: consumer_offset.kind,
                consumer_id: consumer_offset.consumer_id,
                offset: consumer_offset.offset,
                path,
            }
        })
        .collect::<Vec<ConsumerOffset>>();

    Ok(consumer_offsets)
}

pub async fn load(
    config: &SystemConfig,
    db: &Db,
    partition: &mut Partition,
) -> Result<(), IggyError> {
    info!(
            "Loading partition with ID: {} for stream with ID: {} and topic with ID: {}, for path: {} from disk...",
            partition.partition_id, partition.stream_id, partition.topic_id, partition.partition_path
        );
    let dir_entries = fs::read_dir(&partition.partition_path).await;
    if let Err(err) = fs::read_dir(&partition.partition_path)
            .await
            .with_context(|| format!("Failed to read partition with ID: {} for stream with ID: {} and topic with ID: {} and path: {}", partition.partition_id, partition.stream_id, partition.topic_id, partition.partition_path))
        {
            return Err(IggyError::CannotReadPartitions(err));
        }

    let key = get_partition_key(
        partition.stream_id,
        partition.topic_id,
        partition.partition_id,
    );
    let partition_data = match db
        .get(&key)
        .with_context(|| format!("Failed to load partition with key: {}", key))
    {
        Ok(partition_data) => {
            if let Some(partition_data) = partition_data {
                let partition_data = rmp_serde::from_slice::<PartitionData>(&partition_data)
                    .with_context(|| format!("Failed to deserialize partition with key: {}", key));
                if let Err(err) = partition_data {
                    return Err(IggyError::CannotDeserializeResource(err));
                } else {
                    partition_data.unwrap()
                }
            } else {
                return Err(IggyError::ResourceNotFound(key));
            }
        }
        Err(err) => {
            return Err(IggyError::CannotLoadResource(err));
        }
    };

    partition.created_at = partition_data.created_at;

    let consumer_offsets_for_consumer = load_consumer_offsets(
        db,
        config,
        ConsumerKind::Consumer,
        partition.stream_id,
        partition.topic_id,
        partition.partition_id,
    )
    .await?;

    let consumer_offsets_for_group = load_consumer_offsets(
        db,
        config,
        ConsumerKind::ConsumerGroup,
        partition.stream_id,
        partition.topic_id,
        partition.partition_id,
    )
    .await?;

    for consumer_offset in consumer_offsets_for_consumer {
        partition
            .consumer_offsets
            .insert(consumer_offset.consumer_id, consumer_offset);
    }

    for consumer_offset in consumer_offsets_for_group {
        partition
            .consumer_group_offsets
            .insert(consumer_offset.consumer_id, consumer_offset);
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

#[derive(Debug, Serialize, Deserialize)]
struct PartitionData {
    created_at: IggyTimestamp,
}

#[derive(Debug, PartialEq, Clone)]
struct ConsumerOffsetCompat {
    pub key: String,
    pub kind: ConsumerKind,
    pub consumer_id: u32,
    pub offset: u64,
}

fn get_partition_key(stream_id: u32, topic_id: u32, partition_id: u32) -> String {
    format!(
        "streams:{}:topics:{}:partitions:{}",
        stream_id, topic_id, partition_id
    )
}

pub fn get_key_prefix(
    kind: ConsumerKind,
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
) -> String {
    format!("{kind}_offsets:{stream_id}:{topic_id}:{partition_id}")
}
