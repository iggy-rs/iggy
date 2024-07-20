use crate::state::system::TopicState;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::storage::TopicStorage;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use anyhow::Context;
use futures::{future::join_all, lock::Mutex};
use iggy::error::IggyError;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::{collections::HashSet, sync::Arc};
use tracing::{error, info, warn};

#[derive(Debug)]
pub struct FileTopicStorage {
    is_enabled: bool,
}

impl FileTopicStorage {
    pub fn noop() -> Self {
        Self { is_enabled: false }
    }

    pub fn new() -> Self {
        Self { is_enabled: true }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ConsumerGroupData {
    id: u32,
    name: String,
}

impl TopicStorage for FileTopicStorage {
    async fn load(&self, topic: &mut Topic, mut state: TopicState) -> Result<(), IggyError> {
        if self.is_enabled {
            info!("Loading topic {} from disk...", topic);
            if !Path::new(&topic.path).exists() {
                return Err(IggyError::TopicIdNotFound(topic.topic_id, topic.stream_id));
            }

            topic.created_at = state.created_at;
            topic.message_expiry = state.message_expiry;
            topic.compression_algorithm = state.compression_algorithm;
            topic.max_topic_size = state.max_topic_size;
            topic.replication_factor = state.replication_factor.unwrap_or(1);

            for consumer_group in state.consumer_groups.into_values() {
                let consumer_group = ConsumerGroup::new(
                    topic.topic_id,
                    consumer_group.id,
                    &consumer_group.name,
                    topic.get_partitions_count(),
                );
                topic
                    .consumer_groups
                    .insert(consumer_group.group_id, consumer_group);
            }

            let dir_entries = std::fs::read_dir(&topic.partitions_path)
            .with_context(|| format!("Failed to read partition with ID: {} for stream with ID: {} for topic with ID: {} and path: {}",
                                     topic.topic_id, topic.stream_id, topic.topic_id, &topic.partitions_path));
            if let Err(err) = dir_entries {
                return Err(IggyError::CannotReadPartitions(err));
            }

            let mut unloaded_partitions = Vec::new();
            let mut dir_entries = dir_entries.unwrap();
            while let Some(dir_entry) = dir_entries.next() {
                let dir_entry = dir_entry.unwrap();
                let metadata = dir_entry.metadata();
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
                let partition_state = state.partitions.get(&partition_id);
                if partition_state.is_none() {
                    let stream_id = topic.stream_id;
                    let topic_id = topic.topic_id;
                    error!("Partition with ID: '{partition_id}' for stream with ID: '{stream_id}' and topic with ID: '{topic_id}' was not found in state, but exists on disk and will be removed.");
                    if let Err(error) = std::fs::remove_dir_all(&dir_entry.path()) {
                        error!("Cannot remove partition directory: {error}");
                    } else {
                        warn!("Partition with ID: '{partition_id}' for stream with ID: '{stream_id}' and topic with ID: '{topic_id}' was removed.");
                    }
                    continue;
                }

                let partition_state = partition_state.unwrap();
                let partition = Partition::create(
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
                    partition_state.created_at,
                );
                unloaded_partitions.push(partition);
            }

            let state_partition_ids = state.partitions.keys().copied().collect::<HashSet<u32>>();
            let unloaded_partition_ids = unloaded_partitions
                .iter()
                .map(|partition| partition.partition_id)
                .collect::<HashSet<u32>>();
            let missing_ids = state_partition_ids
                .difference(&unloaded_partition_ids)
                .copied()
                .collect::<HashSet<u32>>();
            if missing_ids.is_empty() {
                info!(
                "All partitions for topic with ID: '{}' for stream with ID: '{}' found on disk were found in state.",
                topic.topic_id, topic.stream_id
            );
            } else {
                error!(
                "Partitions with IDs: '{missing_ids:?}' for topic with ID: '{topic_id}' for stream with ID: '{stream_id}' were not found on disk.",
                topic_id = topic.topic_id, stream_id = topic.stream_id
            );
                return Err(IggyError::MissingPartitions(
                    topic.topic_id,
                    topic.stream_id,
                ));
            }

            let stream_id = topic.stream_id;
            let topic_id = topic.topic_id;
            let loaded_partitions = Arc::new(Mutex::new(Vec::new()));
            let mut load_partitions = Vec::new();
            for mut partition in unloaded_partitions {
                let loaded_partitions = loaded_partitions.clone();
                let partition_state = state.partitions.remove(&partition.partition_id).unwrap();
                let load_partition = monoio::spawn(async move {
                    match partition.load(partition_state).await {
                        Ok(_) => {
                            loaded_partitions.lock().await.push(partition);
                        }
                        Err(error) => {
                            error!(
                            "Failed to load partition with ID: {} for stream with ID: {stream_id} and topic with ID: {topic_id}. Error: {error}",
                            partition.partition_id);
                        }
                    }
                });
                load_partitions.push(load_partition);
            }

            join_all(load_partitions).await;
            for partition in loaded_partitions.lock().await.drain(..) {
                topic
                    .partitions
                    .borrow_mut()
                    .insert(partition.partition_id, partition);
            }
            //topic.load_messages_from_disk_to_cache().await?;

            info!("Loaded topic {topic}");
        }

        Ok(())
    }

    async fn save(&self, topic: &Topic) -> Result<(), IggyError> {
        if self.is_enabled {
            if !Path::new(&topic.path).exists() && std::fs::create_dir(&topic.path).is_err() {
                return Err(IggyError::CannotCreateTopicDirectory(
                    topic.topic_id,
                    topic.stream_id,
                    topic.path.clone(),
                ));
            }

            if !Path::new(&topic.partitions_path).exists()
                && std::fs::create_dir(&topic.partitions_path).is_err()
            {
                return Err(IggyError::CannotCreatePartitionsDirectory(
                    topic.stream_id,
                    topic.topic_id,
                ));
            }

            info!(
                "Saving {} partition(s) for topic {topic}...",
                topic.partitions.borrow().len()
            );
            for (_, partition) in topic.partitions.borrow().iter() {
                partition.persist().await?;
            }

            info!("Saved topic {topic}");
        }
        Ok(())
    }

    async fn delete(&self, topic: &Topic) -> Result<(), IggyError> {
        if self.is_enabled {
            info!("Deleting topic {topic}...");
            if std::fs::remove_dir_all(&topic.path).is_err() {
                return Err(IggyError::CannotDeleteTopicDirectory(
                    topic.topic_id,
                    topic.stream_id,
                    topic.path.clone(),
                ));
            }

            info!(
                "Deleted topic with ID: {} for stream with ID: {}.",
                topic.topic_id, topic.stream_id
            );
        }

        Ok(())
    }
}
