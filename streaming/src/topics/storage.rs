use crate::partitions::partition::Partition;
use crate::persister::Persister;
use crate::storage::{Storage, TopicStorage};
use crate::topics::consumer_group::ConsumerGroup;
use crate::topics::topic::Topic;
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
    persister: Arc<dyn Persister>,
}

impl FileTopicStorage {
    pub fn new(db: Arc<Db>, persister: Arc<dyn Persister>) -> Self {
        Self { db, persister }
    }
}

unsafe impl Send for FileTopicStorage {}
unsafe impl Sync for FileTopicStorage {}

#[async_trait]
impl TopicStorage for FileTopicStorage {
    async fn save_partitions(&self, topic: &Topic, partition_ids: &[u32]) -> Result<(), Error> {
        for partition_id in partition_ids {
            if !topic.partitions.contains_key(partition_id) {
                return Err(Error::PartitionNotFound(
                    *partition_id,
                    topic.topic_id,
                    topic.stream_id,
                ));
            }
        }

        for partition_id in partition_ids {
            let partition = topic.partitions.get(partition_id).unwrap();
            let partition = partition.read().await;
            partition.persist().await?;
        }

        Ok(())
    }

    async fn delete_partitions(
        &self,
        topic: &mut Topic,
        partition_ids: &[u32],
    ) -> Result<(), Error> {
        for partition_id in partition_ids {
            if !topic.partitions.contains_key(partition_id) {
                return Err(Error::PartitionNotFound(
                    *partition_id,
                    topic.topic_id,
                    topic.stream_id,
                ));
            }
        }

        for partition_id in partition_ids {
            let partition = topic.partitions.get(partition_id).unwrap();
            let partition = partition.read().await;
            partition.delete().await?;
        }

        Ok(())
    }

    async fn save_consumer_group(
        &self,
        topic: &Topic,
        consumer_group: &ConsumerGroup,
    ) -> Result<(), Error> {
        if self
            .persister
            .overwrite(
                &topic.get_consumer_group_path(consumer_group.id),
                &consumer_group.id.to_le_bytes(),
            )
            .await
            .is_err()
        {
            return Err(Error::CannotCreateConsumerGroupInfo(
                consumer_group.id,
                topic.topic_id,
                topic.stream_id,
            ));
        }

        info!(
            "Consumer group with ID: {} for topic with ID: {} and stream with ID: {} was saved.",
            consumer_group.id, topic.topic_id, topic.stream_id
        );

        Ok(())
    }

    async fn load_consumer_groups(&self, topic: &mut Topic) -> Result<(), Error> {
        info!(
            "Loading consumer groups for topic with ID: {} for stream with ID: {} from disk...",
            topic.topic_id, topic.stream_id
        );

        let dir_entries = fs::read_dir(&topic.get_consumer_groups_path()).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadConsumerGroups(
                topic.topic_id,
                topic.stream_id,
            ));
        }

        let mut dir_entries = dir_entries.unwrap();
        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
            let metadata = dir_entry.metadata().await;
            if metadata.is_err() || metadata.unwrap().is_dir() {
                continue;
            }

            let name = dir_entry.file_name().into_string().unwrap();
            let consumer_group_id = name.parse::<u32>();
            if consumer_group_id.is_err() {
                error!("Invalid consumer group ID file with name: '{}'.", name);
                continue;
            }

            let consumer_group_id = consumer_group_id.unwrap();
            topic.consumer_groups.insert(
                consumer_group_id,
                RwLock::new(ConsumerGroup::new(
                    topic.topic_id,
                    consumer_group_id,
                    topic.partitions.len() as u32,
                )),
            );
        }

        Ok(())
    }

    async fn delete_consumer_group(
        &self,
        topic: &Topic,
        consumer_group: &ConsumerGroup,
    ) -> Result<(), Error> {
        if self
            .persister
            .delete(&topic.get_consumer_group_path(consumer_group.id))
            .await
            .is_err()
        {
            return Err(Error::CannotDeleteConsumerGroupInfo(
                consumer_group.id,
                topic.topic_id,
                topic.stream_id,
            ));
        }

        info!(
            "Consumer group with ID: {} for topic with ID: {} and stream with ID: {} was deleted.",
            consumer_group.id, topic.topic_id, topic.stream_id
        );

        Ok(())
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

        let key = get_key(topic.stream_id, topic.topic_id);
        let topic_data = self.db.get(&key);
        if topic_data.is_err() {
            return Err(Error::CannotLoadResource(key));
        }

        let topic_data = topic_data.unwrap();
        if topic_data.is_none() {
            return Err(Error::ResourceNotFound(key));
        }

        let topic_data = topic_data.unwrap();
        let topic_data = rmp_serde::from_slice::<TopicData>(&topic_data);
        if topic_data.is_err() {
            return Err(Error::CannotDeserializeResource(key));
        }

        let topic_data = topic_data.unwrap();
        topic.name = topic_data.name;
        topic.created_at = topic_data.created_at;
        topic.message_expiry = topic_data.message_expiry;

        let dir_entries = fs::read_dir(&topic.partitions_path).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadPartitions(topic.topic_id, topic.stream_id));
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
                if partition.load().await.is_err() {
                    error!("Failed to load partition with ID: {} for stream with ID: {} and topic with ID: {}", partition.partition_id, stream_id, topic_id);
                    return;
                }

                loaded_partitions.lock().await.push(partition);
            });
            load_partitions.push(load_partition);
        }

        join_all(load_partitions).await;
        for partition in loaded_partitions.lock().await.drain(..) {
            topic
                .partitions
                .insert(partition.partition_id, RwLock::new(partition));
        }

        self.load_consumer_groups(topic).await?;
        topic.load_messages_to_cache().await?;

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

        if !Path::new(&topic.get_consumer_groups_path()).exists()
            && create_dir(&topic.get_consumer_groups_path()).await.is_err()
        {
            return Err(Error::CannotCreateConsumerGroupsDirectory(
                topic.stream_id,
                topic.topic_id,
            ));
        }

        let key = get_key(topic.stream_id, topic.topic_id);
        match rmp_serde::to_vec(&TopicData {
            name: topic.name.clone(),
            created_at: topic.created_at,
            message_expiry: topic.message_expiry,
        }) {
            Ok(data) => {
                if let Err(err) = self.db.insert(&key, data) {
                    error!(
                        "Cannot save topic with ID: {} for stream with ID: {}. Error: {}",
                        topic.topic_id, topic.stream_id, err
                    );
                    return Err(Error::CannotSaveResource(key.to_string()));
                }
            }
            Err(err) => {
                error!(
                    "Cannot serialize topic with ID: {} for stream with ID: {}. Error: {}",
                    topic.topic_id, topic.stream_id, err
                );
                return Err(Error::CannotSerializeResource(key));
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
        let key = get_key(topic.stream_id, topic.topic_id);
        if self.db.remove(&key).is_err() {
            return Err(Error::CannotDeleteResource(key));
        }
        if fs::remove_dir_all(&topic.path).await.is_err() {
            return Err(Error::CannotDeleteTopicDirectory(
                topic.topic_id,
                topic.stream_id,
            ));
        }

        info!(
            "Deleted topic with ID: {} for stream with ID: {}.",
            topic.topic_id, topic.stream_id
        );

        Ok(())
    }
}

fn get_key(stream_id: u32, topic_id: u32) -> String {
    format!("streams:{}:topics:{}", stream_id, topic_id)
}
