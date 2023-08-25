use crate::partitions::partition::Partition;
use crate::persister::Persister;
use crate::storage::{Storage, TopicStorage};
use crate::topics::consumer_group::ConsumerGroup;
use crate::topics::topic::Topic;
use crate::utils::file;
use async_trait::async_trait;
use futures::future::join_all;
use iggy::error::Error;
use iggy::utils::text;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::create_dir;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info};

#[derive(Debug)]
pub struct FileTopicStorage {
    persister: Arc<dyn Persister>,
}

impl FileTopicStorage {
    pub fn new(persister: Arc<dyn Persister>) -> Self {
        Self { persister }
    }
}

unsafe impl Send for FileTopicStorage {}
unsafe impl Sync for FileTopicStorage {}

#[async_trait]
impl TopicStorage for FileTopicStorage {
    async fn save_partitions(&self, topic: &Topic, partition_ids: &[u32]) -> Result<(), Error> {
        for partition_id in partition_ids {
            if !topic.partitions.contains_key(partition_id) {
                return Err(Error::PartitionNotFound(*partition_id));
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
                return Err(Error::PartitionNotFound(*partition_id));
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
                topic.id,
                topic.stream_id,
            ));
        }

        info!(
            "Consumer group with ID: {} for topic with ID: {} and stream with ID: {} was saved.",
            consumer_group.id, topic.id, topic.stream_id
        );

        Ok(())
    }

    async fn load_consumer_groups(&self, topic: &mut Topic) -> Result<(), Error> {
        info!(
            "Loading consumer groups for topic with ID: {} for stream with ID: {} from disk...",
            topic.id, topic.stream_id
        );

        let dir_entries = fs::read_dir(&topic.get_consumer_groups_path()).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadConsumerGroups(topic.id, topic.stream_id));
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
                    topic.id,
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
                topic.id,
                topic.stream_id,
            ));
        }

        info!(
            "Consumer group with ID: {} for topic with ID: {} and stream with ID: {} was deleted.",
            consumer_group.id, topic.id, topic.stream_id
        );

        Ok(())
    }
}

#[async_trait]
impl Storage<Topic> for FileTopicStorage {
    async fn load(&self, topic: &mut Topic) -> Result<(), Error> {
        info!(
            "Loading topic with ID: {} for stream with ID: {} from disk...",
            topic.id, topic.stream_id
        );
        if !Path::new(&topic.path).exists() {
            return Err(Error::TopicIdNotFound(topic.id, topic.stream_id));
        }

        let topic_info_file = file::open(&topic.info_path).await;
        if topic_info_file.is_err() {
            return Err(Error::CannotOpenTopicInfo(topic.id, topic.stream_id));
        }

        let mut topic_info = String::new();
        if topic_info_file
            .unwrap()
            .read_to_string(&mut topic_info)
            .await
            .is_err()
        {
            return Err(Error::CannotReadTopicInfo(topic.id, topic.stream_id));
        }

        let lines = topic_info.lines().collect::<Vec<&str>>();
        let topic_name = text::to_lowercase_non_whitespace(lines.get(0).unwrap());
        let message_expiry = match lines.get(1) {
            Some(line) => line.parse::<u32>().ok(),
            None => None,
        };

        topic.name = topic_name;
        topic.message_expiry = message_expiry;
        let dir_entries = fs::read_dir(&topic.get_partitions_path()).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadPartitions(topic.id, topic.stream_id));
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
                topic.id,
                partition_id,
                &topic.get_partitions_path(),
                false,
                topic.config.clone(),
                topic.storage.clone(),
                topic.message_expiry,
            );
            unloaded_partitions.push(partition);
        }

        let stream_id = topic.stream_id;
        let topic_id = topic.id;
        let loaded_partitions = Arc::new(Mutex::new(Vec::new()));
        let mut load_partitions = Vec::new();
        for mut partition in unloaded_partitions {
            let loaded_partitions = loaded_partitions.clone();
            let load_partition = tokio::spawn(async move {
                if partition.load().await.is_err() {
                    error!("Failed to load partition with ID: {} for stream with ID: {} and topic with ID: {}", partition.id, stream_id, topic_id);
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
                .insert(partition.id, RwLock::new(partition));
        }

        self.load_consumer_groups(topic).await?;
        topic.load_messages_to_cache().await?;

        info!(
            "Loaded topic: '{}' with ID: {} for stream with ID: {} from disk. Message expiry: {:?}",
            &topic.name, &topic.id, topic.stream_id, topic.message_expiry
        );

        Ok(())
    }

    async fn save(&self, topic: &Topic) -> Result<(), Error> {
        if Path::new(&topic.path).exists() {
            return Err(Error::TopicIdAlreadyExists(topic.id, topic.stream_id));
        }

        if create_dir(&topic.path).await.is_err() {
            return Err(Error::CannotCreateTopicDirectory(topic.id, topic.stream_id));
        }

        if create_dir(&topic.get_partitions_path()).await.is_err() {
            return Err(Error::CannotCreatePartitionsDirectory(
                topic.stream_id,
                topic.id,
            ));
        }

        if create_dir(&topic.get_consumer_groups_path()).await.is_err() {
            return Err(Error::CannotCreateConsumerGroupsDirectory(
                topic.stream_id,
                topic.id,
            ));
        }

        let topic_info = format!("{}\n{}", topic.name, topic.message_expiry.unwrap_or(0));
        if self
            .persister
            .overwrite(&topic.info_path, topic_info.as_bytes())
            .await
            .is_err()
        {
            return Err(Error::CannotCreateTopicInfo(topic.id, topic.stream_id));
        }

        info!(
            "Topic with ID: {} was saved, path: {}",
            topic.id, topic.path
        );

        info!(
            "Creating {} partition(s) for topic with ID: {} and stream with ID: {}...",
            topic.partitions.len(),
            topic.id,
            topic.stream_id
        );
        for (_, partition) in topic.partitions.iter() {
            let partition = partition.write().await;
            partition.persist().await?;
        }

        Ok(())
    }

    async fn delete(&self, topic: &Topic) -> Result<(), Error> {
        info!(
            "Deleting topic with ID: {} for stream with ID: {}...",
            topic.id, topic.stream_id
        );
        if fs::remove_dir_all(&topic.path).await.is_err() {
            return Err(Error::CannotDeleteTopicDirectory(topic.id, topic.stream_id));
        }

        info!(
            "Deleted topic with ID: {} for stream with ID: {}.",
            topic.id, topic.stream_id
        );

        Ok(())
    }
}
