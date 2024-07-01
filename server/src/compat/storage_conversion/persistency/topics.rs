use crate::compat::storage_conversion::persistency::partitions;
use crate::configs::system::SystemConfig;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use anyhow::Context;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::error::IggyError;
use iggy::locking::IggySharedMut;
use iggy::locking::IggySharedMutFn;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::duration::{IggyDuration, SEC_IN_MICRO};
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::path::Path;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{error, info};

#[derive(Debug, Serialize, Deserialize)]
struct ConsumerGroupData {
    id: u32,
    name: String,
}

pub async fn load(config: &SystemConfig, db: &Db, topic: &mut Topic) -> Result<(), IggyError> {
    info!("Loading topic {} from disk...", topic);
    if !Path::new(&topic.path).exists() {
        return Err(IggyError::TopicIdNotFound(topic.topic_id, topic.stream_id));
    }

    let key = get_topic_key(topic.stream_id, topic.topic_id);
    let topic_data = match db
        .get(&key)
        .with_context(|| format!("Failed to load topic with key: {}", key))
    {
        Ok(data) => {
            if let Some(topic_data) = data {
                let topic_data = rmp_serde::from_slice::<TopicData>(&topic_data)
                    .with_context(|| format!("Failed to deserialize topic with key: {}", key));
                if let Err(err) = topic_data {
                    return Err(IggyError::CannotDeserializeResource(err));
                } else {
                    topic_data.unwrap()
                }
            } else {
                return Err(IggyError::ResourceNotFound(key));
            }
        }
        Err(err) => {
            return Err(IggyError::CannotLoadResource(err));
        }
    };

    topic.name = topic_data.name;
    topic.created_at = topic_data.created_at;
    topic.message_expiry = match topic_data.message_expiry {
        Some(expiry) => {
            IggyExpiry::ExpireDuration(IggyDuration::from(expiry as u64 * SEC_IN_MICRO))
        }
        None => IggyExpiry::NeverExpire,
    };
    topic.compression_algorithm = topic_data.compression_algorithm;
    topic.max_topic_size = topic_data.max_topic_size.into();
    topic.replication_factor = topic_data.replication_factor;

    let dir_entries = fs::read_dir(&topic.partitions_path).await
            .with_context(|| format!("Failed to read partition with ID: {} for stream with ID: {} for topic with ID: {} and path: {}",
                                     topic.topic_id, topic.stream_id, topic.topic_id, &topic.partitions_path));
    if let Err(err) = dir_entries {
        return Err(IggyError::CannotReadPartitions(err));
    }

    let mut dir_entries = dir_entries.unwrap();
    while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
        let metadata = dir_entry.metadata().await;
        if metadata.is_err() || metadata.unwrap().is_file() {
            continue;
        }

        let name = dir_entry.file_name().into_string().unwrap();
        let partition_id = name.parse::<u32>();
        if partition_id.is_err() {
            error!("Invalid partition ID file with name: '{}'.", name);
            continue;
        }

        let partition_id = partition_id.unwrap();
        let mut partition = Partition::create(
            topic.stream_id,
            topic.topic_id,
            partition_id,
            false,
            topic.config.clone(),
            topic.storage.clone(),
            topic.message_expiry,
            topic.messages_count_of_parent_stream.clone(),
            topic.messages_count.clone(),
            topic.size_of_parent_stream.clone(),
            topic.size_bytes.clone(),
            topic.segments_count_of_parent_stream.clone(),
            IggyTimestamp::zero(),
        );
        partitions::load(config, db, &mut partition).await?;
        topic
            .partitions
            .insert(partition.partition_id, IggySharedMut::new(partition));
    }

    let consumer_groups = load_consumer_groups(db, topic).await?;
    topic.consumer_groups = consumer_groups
        .into_iter()
        .map(|group| (group.group_id, RwLock::new(group)))
        .collect();
    info!("Loaded topic {topic}");
    Ok(())
}

pub async fn load_consumer_groups(db: &Db, topic: &Topic) -> Result<Vec<ConsumerGroup>, IggyError> {
    info!("Loading consumer groups for topic {} from disk...", topic);
    let key_prefix = get_consumer_groups_key_prefix(topic.stream_id, topic.topic_id);
    let mut consumer_groups = Vec::new();
    for data in db.scan_prefix(format!("{}:", key_prefix)) {
        let consumer_group = match data.with_context(|| {
            format!(
                "Failed to load consumer group when searching for key: {}",
                key_prefix
            )
        }) {
            Ok((_, value)) => {
                match rmp_serde::from_slice::<ConsumerGroupData>(&value).with_context(|| {
                    format!(
                        "Failed to deserialize consumer group with key: {}",
                        key_prefix
                    )
                }) {
                    Ok(user) => user,
                    Err(err) => {
                        return Err(IggyError::CannotDeserializeResource(err));
                    }
                }
            }
            Err(err) => {
                return Err(IggyError::CannotLoadResource(err));
            }
        };
        let consumer_group = ConsumerGroup::new(
            topic.topic_id,
            consumer_group.id,
            &consumer_group.name,
            topic.get_partitions_count(),
        );
        consumer_groups.push(consumer_group);
    }
    info!(
        "Loaded {} consumer groups for topic {}",
        consumer_groups.len(),
        topic
    );
    Ok(consumer_groups)
}

#[derive(Debug, Serialize, Deserialize)]
struct TopicData {
    name: String,
    created_at: IggyTimestamp,
    message_expiry: Option<u32>,
    compression_algorithm: CompressionAlgorithm,
    max_topic_size: Option<IggyByteSize>,
    replication_factor: u8,
}

fn get_topic_key(stream_id: u32, topic_id: u32) -> String {
    format!("streams:{}:topics:{}", stream_id, topic_id)
}

fn get_consumer_groups_key_prefix(stream_id: u32, topic_id: u32) -> String {
    format!("streams:{stream_id}:topics:{topic_id}:consumer_groups")
}
