use crate::streaming::partitions::partition::Partition;
use crate::streaming::storage::{Storage, TopicStorage};
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use anyhow::Context;
use async_trait::async_trait;
use futures::future::join_all;
use iggy::error::Error;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::create_dir;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info};

#[derive(Debug)]
pub struct FileTopicStorage {
    db: Arc<Db>,
}

impl FileTopicStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

unsafe impl Send for FileTopicStorage {}
unsafe impl Sync for FileTopicStorage {}

#[derive(Debug, Serialize, Deserialize)]
struct ConsumerGroupData {
    id: u32,
    name: String,
}

#[async_trait]
impl TopicStorage for FileTopicStorage {
    async fn save_consumer_group(
        &self,
        topic: &Topic,
        consumer_group: &ConsumerGroup,
    ) -> Result<(), Error> {
        let key = get_consumer_group_key(
            topic.stream_id,
            topic.topic_id,
            consumer_group.consumer_group_id,
        );
        match rmp_serde::to_vec(&ConsumerGroupData {
            id: consumer_group.consumer_group_id,
            name: consumer_group.name.clone(),
        })
        .with_context(|| format!("Failed to serialize consumer group with key: {}", key))
        {
            Ok(data) => {
                if let Err(err) = self
                    .db
                    .insert(&key, data)
                    .with_context(|| format!("Failed to insert consumer group with key: {}", key))
                {
                    return Err(Error::CannotSaveResource(err));
                }
            }
            Err(err) => {
                return Err(Error::CannotSerializeResource(err));
            }
        }

        Ok(())
    }

    async fn load_consumer_groups(&self, topic: &Topic) -> Result<Vec<ConsumerGroup>, Error> {
        info!(
            "Loading consumer groups for topic with ID: {} for stream with ID: {} from disk...",
            topic.topic_id, topic.stream_id
        );

        let key_prefix = get_consumer_groups_key_prefix(topic.stream_id, topic.topic_id);
        let mut consumer_groups = Vec::new();
        for data in self.db.scan_prefix(format!("{}:", key_prefix)) {
            let consumer_group = match data.with_context(|| {
                format!(
                    "Failed to load consumer group when searching for key: {}",
                    key_prefix
                )
            }) {
                Ok((_, value)) => match rmp_serde::from_slice::<ConsumerGroupData>(&value)
                    .with_context(|| {
                        format!(
                            "Failed to deserialize consumer group with key: {}",
                            key_prefix
                        )
                    }) {
                    Ok(user) => user,
                    Err(err) => {
                        return Err(Error::CannotDeserializeResource(err));
                    }
                },
                Err(err) => {
                    return Err(Error::CannotLoadResource(err));
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

        Ok(consumer_groups)
    }

    async fn delete_consumer_group(
        &self,
        topic: &Topic,
        consumer_group: &ConsumerGroup,
    ) -> Result<(), Error> {
        let key = get_consumer_group_key(
            topic.stream_id,
            topic.topic_id,
            consumer_group.consumer_group_id,
        );
        match self
            .db
            .remove(&key)
            .with_context(|| format!("Failed to delete consumer group with key: {}", key))
        {
            Ok(_) => {
                info!(
            "Consumer group with ID: {} for topic with ID: {} and stream with ID: {} was deleted.",
            consumer_group.consumer_group_id, topic.topic_id, topic.stream_id
        );
                Ok(())
            }
            Err(err) => {
                return Err(Error::CannotDeleteResource(err));
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TopicData {
    name: String,
    created_at: u64,
    message_expiry: Option<u32>,
}

#[async_trait]
impl Storage<Topic> for FileTopicStorage {
    async fn load(&self, topic: &mut Topic) -> Result<(), Error> {
        info!(
            "Loading topic with ID: {} for stream with ID: {} from disk...",
            topic.topic_id, topic.stream_id
        );
        if !Path::new(&topic.path).exists() {
            return Err(Error::TopicIdNotFound(topic.topic_id, topic.stream_id));
        }

        let key = get_topic_key(topic.stream_id, topic.topic_id);
        let topic_data = match self
            .db
            .get(&key)
            .with_context(|| format!("Failed to load topic with key: {}", key))
        {
            Ok(data) => {
                if let Some(topic_data) = data {
                    let topic_data = rmp_serde::from_slice::<TopicData>(&topic_data)
                        .with_context(|| format!("Failed to deserialize topic with key: {}", key));
                    if let Err(err) = topic_data {
                        return Err(Error::CannotDeserializeResource(err));
                    } else {
                        topic_data.unwrap()
                    }
                } else {
                    return Err(Error::ResourceNotFound(key));
                }
            }
            Err(err) => {
                return Err(Error::CannotLoadResource(err));
            }
        };

        topic.name = topic_data.name;
        topic.created_at = topic_data.created_at;
        topic.message_expiry = topic_data.message_expiry;

        let dir_entries = fs::read_dir(&topic.partitions_path).await
            .with_context(|| format!("Failed to read partition with ID: {} for stream with ID: {} for topic with ID: {} and path: {}", 
            topic.topic_id, topic.stream_id, topic.topic_id, &topic.partitions_path));
        if let Err(err) = dir_entries {
            return Err(Error::CannotReadPartitions(err));
        }

        let mut unloaded_partitions = Vec::new();
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
            let partition = Partition::create(
                topic.stream_id,
                topic.topic_id,
                partition_id,
                false,
                topic.config.clone(),
                topic.storage.clone(),
                topic.message_expiry,
            );
            unloaded_partitions.push(partition);
        }

        let stream_id = topic.stream_id;
        let topic_id = topic.topic_id;
        let loaded_partitions = Arc::new(Mutex::new(Vec::new()));
        let mut load_partitions = Vec::new();
        for mut partition in unloaded_partitions {
            let loaded_partitions = loaded_partitions.clone();
            let load_partition = tokio::spawn(async move {
                match partition.load().await {
                    Ok(_) => {
                        loaded_partitions.lock().await.push(partition);
                    }
                    Err(error) => {
                        error!(
                            "Failed to load partition with ID: {} for stream with ID: {} and topic with ID: {}. Error: {}",
                            partition.partition_id, stream_id, topic_id, error);
                    }
                }
            });
            load_partitions.push(load_partition);
        }

        join_all(load_partitions).await;
        for partition in loaded_partitions.lock().await.drain(..) {
            topic
                .partitions
                .insert(partition.partition_id, Arc::new(RwLock::new(partition)));
        }

        self.load_consumer_groups(topic).await?;
        topic.load_messages_from_disk_to_cache().await?;

        info!(
            "Loaded topic: '{}' with ID: {} for stream with ID: {} from disk. Message expiry: {:?}",
            &topic.name, &topic.topic_id, topic.stream_id, topic.message_expiry
        );

        Ok(())
    }

    async fn save(&self, topic: &Topic) -> Result<(), Error> {
        if !Path::new(&topic.path).exists() && create_dir(&topic.path).await.is_err() {
            return Err(Error::CannotCreateTopicDirectory(
                topic.topic_id,
                topic.stream_id,
                topic.path.clone(),
            ));
        }

        if !Path::new(&topic.partitions_path).exists()
            && create_dir(&topic.partitions_path).await.is_err()
        {
            return Err(Error::CannotCreatePartitionsDirectory(
                topic.stream_id,
                topic.topic_id,
            ));
        }

        let key = get_topic_key(topic.stream_id, topic.topic_id);
        match rmp_serde::to_vec(&TopicData {
            name: topic.name.clone(),
            created_at: topic.created_at,
            message_expiry: topic.message_expiry,
        })
        .with_context(|| format!("Failed to serialize topic with key: {}", key))
        {
            Ok(data) => {
                if let Err(err) = self
                    .db
                    .insert(&key, data)
                    .with_context(|| format!("Failed to insert topic with key: {}", key))
                {
                    return Err(Error::CannotSaveResource(err));
                }
            }
            Err(err) => {
                return Err(Error::CannotSerializeResource(err));
            }
        }

        info!(
            "Saving {} partition(s) for topic with ID: {} and stream with ID: {}...",
            topic.partitions.len(),
            topic.topic_id,
            topic.stream_id
        );
        for (_, partition) in topic.partitions.iter() {
            let partition = partition.write().await;
            partition.persist().await?;
        }

        info!(
            "Saved topic with ID: {} for stream with ID: {}, path: {}",
            topic.topic_id, topic.stream_id, topic.path
        );

        Ok(())
    }

    async fn delete(&self, topic: &Topic) -> Result<(), Error> {
        info!(
            "Deleting topic with ID: {} for stream with ID: {}...",
            topic.topic_id, topic.stream_id
        );
        let key = get_topic_key(topic.stream_id, topic.topic_id);
        //I END HERE
        if let Err(err) = self
            .db
            .remove(&key)
            .with_context(|| format!("Failed to delete topic with key: {}", key))
        {
            return Err(Error::CannotDeleteResource(err));
        }
        for consumer_group in topic.consumer_groups.values() {
            let consumer_group = consumer_group.read().await;
            self.delete_consumer_group(topic, &consumer_group).await?;
        }
        if fs::remove_dir_all(&topic.path).await.is_err() {
            return Err(Error::CannotDeleteTopicDirectory(
                topic.topic_id,
                topic.stream_id,
                topic.path.clone(),
            ));
        }

        info!(
            "Deleted topic with ID: {} for stream with ID: {}.",
            topic.topic_id, topic.stream_id
        );

        Ok(())
    }
}

fn get_topic_key(stream_id: u32, topic_id: u32) -> String {
    format!("streams:{}:topics:{}", stream_id, topic_id)
}

fn get_consumer_group_key(stream_id: u32, topic_id: u32, consumer_group_id: u32) -> String {
    format!(
        "{}:{}",
        get_consumer_groups_key_prefix(stream_id, topic_id),
        consumer_group_id
    )
}

fn get_consumer_groups_key_prefix(stream_id: u32, topic_id: u32) -> String {
    format!("streams:{}:topics:{}:consumer_groups", stream_id, topic_id)
}
