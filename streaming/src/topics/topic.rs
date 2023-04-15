use crate::config::TopicConfig;
use crate::error::Error;
use crate::message::Message;
use crate::partitions::partition::Partition;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

const TOPIC_INFO: &str = "topic.info";

#[derive(Debug)]
pub struct Topic {
    pub id: u32,
    pub name: String,
    pub path: String,
    info_path: String,
    config: Arc<TopicConfig>,
    partitions: HashMap<u32, Partition>,
}

impl Topic {
    pub fn empty(id: u32, topics_path: &str, config: Arc<TopicConfig>) -> Topic {
        Topic::create(id, topics_path, "", 0, config)
    }

    pub fn create(
        id: u32,
        topics_path: &str,
        name: &str,
        partitions_count: u32,
        config: Arc<TopicConfig>,
    ) -> Topic {
        let path = format!("{}/{}", topics_path, id);
        let info_path = format!("{}/{}", path, TOPIC_INFO);

        let mut topic = Topic {
            id,
            name: name.to_string(),
            partitions: HashMap::new(),
            path,
            info_path,
            config: config.clone(),
        };

        topic.partitions = (0..partitions_count)
            .map(|id| {
                let partition = Partition::create(id, &topic.path, true, config.partition.clone());
                (id, partition)
            })
            .collect();

        topic
    }

    pub fn get_partitions(&self) -> Vec<&Partition> {
        self.partitions.values().collect()
    }

    pub fn get_partitions_mut(&mut self) -> Vec<&mut Partition> {
        self.partitions.values_mut().collect()
    }

    pub async fn persist_messages(&mut self) -> Result<(), Error> {
        for partition in self.get_partitions_mut() {
            for segment in partition.get_segments_mut() {
                segment.persist_messages().await?;
            }
        }

        Ok(())
    }

    pub async fn persist(&mut self) -> Result<(), Error> {
        if Path::new(&self.path).exists() {
            return Err(Error::TopicAlreadyExists(self.id));
        }

        if std::fs::create_dir(&self.path).is_err() {
            return Err(Error::CannotCreateTopicDirectory(self.id));
        }

        info!("Topic with ID {} was saved, path: {}", self.id, self.path);

        let topic_info_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.info_path)
            .await;

        if topic_info_file.is_err() {
            return Err(Error::CannotCreateTopicInfo(self.id));
        }

        if topic_info_file
            .unwrap()
            .write_all(self.name.as_bytes())
            .await
            .is_err()
        {
            return Err(Error::CannotUpdateTopicInfo(self.id));
        }

        info!(
            "Creating {} partition(s) for topic with ID: {}...",
            self.partitions.len(),
            &self.id
        );
        for partition in self.partitions.iter_mut() {
            if std::fs::create_dir(&partition.1.path).is_err() {
                return Err(Error::CannotCreatePartitionDirectory(*partition.0, self.id));
            }

            partition.1.persist().await?;
            info!(
                "Partition with ID {} for topic with ID: {} was saved, path: {}",
                partition.0, &self.id, partition.1.path
            );
        }

        Ok(())
    }

    pub async fn delete(&self) -> Result<(), Error> {
        info!("Deleting topic directory with ID: {}...", &self.id);
        if fs::remove_dir_all(&self.path).await.is_err() {
            return Err(Error::CannotDeleteTopicDirectory(self.id));
        }

        Ok(())
    }

    pub async fn append_messages(
        &mut self,
        partition_id: u32,
        message: Message,
    ) -> Result<(), Error> {
        let partition = self.partitions.get_mut(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        partition.append_messages(message).await?;
        Ok(())
    }

    pub fn get_messages(
        &self,
        partition_id: u32,
        offset: u64,
        count: u32,
    ) -> Result<Vec<&Message>, Error> {
        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(partition_id));
        }

        let partition = partition.unwrap();
        let messages = partition.get_messages(offset, count);
        if messages.is_none() {
            return Err(Error::MessagesNotFound);
        }

        let messages = messages.unwrap();
        if messages.is_empty() {
            return Err(Error::MessagesNotFound);
        }

        Ok(messages)
    }

    pub async fn load(&mut self) -> Result<(), Error> {
        info!("Loading topic with ID: {} from disk...", &self.id);
        if !Path::new(&self.path).exists() {
            return Err(Error::TopicNotFound(self.id));
        }

        let topic_info_file = OpenOptions::new().read(true).open(&self.info_path).await;
        if topic_info_file.is_err() {
            return Err(Error::CannotOpenTopicInfo(self.id));
        }

        let mut topic_info = String::new();
        if topic_info_file
            .unwrap()
            .read_to_string(&mut topic_info)
            .await
            .is_err()
        {
            return Err(Error::CannotReadTopicInfo(self.id));
        }

        self.name = topic_info;
        let dir_files = fs::read_dir(&self.path).await;
        if dir_files.is_err() {
            return Err(Error::CannotReadPartitions(self.id));
        }

        let mut dir_files = dir_files.unwrap();
        loop {
            let dir_entry = dir_files.next_entry().await;
            if dir_entry.is_err() {
                break;
            }

            let dir_entry = dir_entry.unwrap();
            if dir_entry.is_none() {
                break;
            }

            let dir_entry = dir_entry.unwrap();
            let metadata = dir_entry.metadata().await;
            if metadata.is_err() {
                continue;
            }

            if metadata.unwrap().is_file() {
                continue;
            }

            let id = dir_entry
                .file_name()
                .to_str()
                .unwrap()
                .parse::<u32>()
                .unwrap();
            let mut partition =
                Partition::create(id, &self.path, false, self.config.partition.clone());
            partition.load().await?;
            self.partitions.insert(partition.id, partition);
        }

        info!(
            "Loaded topic: '{}' with ID: {} from disk.",
            &self.name, &self.id
        );

        Ok(())
    }
}
