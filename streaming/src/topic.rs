use std::collections::HashMap;
use std::path::Path;
use tokio::{fs};
use tokio::fs::{OpenOptions};
use tracing::{error, info};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::{get_topics_path, TOPIC_INFO};
use crate::message::Message;
use crate::partition::Partition;
use crate::stream_error::StreamError;


#[derive(Debug)]
pub struct Topic {
    pub id: u32,
    pub name: String,
    pub messages_count: u64,
    pub partitions: HashMap<u32, Partition>,
    pub path: String
}

impl Topic {
    pub fn create(id: u32, name: &str, partitions_count: u32) -> Topic {
        let mut topic = Topic {
            id,
            name: name.to_string(),
            messages_count: 0,
            partitions: HashMap::new(),
            path: format!("{}/{:0>10}", get_topics_path(), id)
        };

        topic.partitions = (0..partitions_count).map(|id| {
            let path = format!("{}/{:0>10}", topic.path, id);
            let partition = Partition::create(id, path, true);
            (id, partition)
        }).collect();

        topic
    }

    pub async fn save_on_disk(&self) -> Result<(), StreamError> {
        if Path::new(&self.path).exists() {
            info!("Topic with ID {} already exists.", self.id);
            return Err(StreamError::TopicAlreadyExists);
        }

        if std::fs::create_dir(&self.path).is_err() {
            error!("Failed to create directory for topic with ID {}.", self.id);
            return Err(StreamError::CannotCreateTopicDirectory);
        }

        info!("Topic with ID {} was saved, path: {}", self.id, self.path);

        let topic_info_path = &format!("{}/{}", &self.path, TOPIC_INFO);
        let topic_info_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(topic_info_path)
            .await;

        if topic_info_file.is_err() {
            error!("Failed to create topic info file for topic with ID {}.", self.id);
            return Err(StreamError::CannotCreateTopicInfo);
        }

        if topic_info_file.unwrap().write_all(self.name.as_bytes()).await.is_err() {
            error!("Failed to write topic info for topic with ID {}.", self.id);
            return Err(StreamError::CannotCreateTopicInfo);
        }

        info!("Creating {} partition(s) for topic with ID: {}...", self.partitions.len(), &self.id);
        for partition in self.partitions.iter() {
            if std::fs::create_dir(&partition.1.path).is_err() {
                error!("Failed to create directory for partition with ID {} for topic with ID {}.", partition.0, self.id);
                return Err(StreamError::CannotCreatePartitionDirectory);
            }

            partition.1.save_on_disk().await?;
            info!("Partition with ID {} for topic with ID: {} was saved, path: {}", partition.0, &self.id, partition.1.path);
        }

        Ok(())
    }

    pub async fn delete(&self) -> Result<(), StreamError> {
        info!("Deleting topic directory with ID: {}...", &self.id);
        if fs::remove_dir_all(&self.path).await.is_err() {
            error!("Failed to delete topic directory with ID: {}.", &self.id);
            return Err(StreamError::CannotDeleteTopicDirectory);
        }

        Ok(())
    }

    pub async fn send_message(&mut self, partition_id: u32, message: Message) -> Result<(), StreamError>{
        let partition = self.partitions.get_mut(&partition_id);
        if partition.is_none() {
            error!("Partition with ID {} was not found.", partition_id);
            return Ok(());
        }

        let partition = partition.unwrap();
        partition.append_message(message).await?;
        Ok(())
    }

    pub fn get_messages(&self, partition_id: u32, offset: u64, count: u32) -> Result<Vec<&Message>, StreamError> {
        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            error!("Partition with ID {} was not found.", partition_id);
            return Err(StreamError::PartitionNotFound);
        }

        let partition = partition.unwrap();
        let messages = partition.get_messages(offset, count);
        if messages.is_none() {
            return Err(StreamError::MessagesNotFound);
        }

        let messages = messages.unwrap();
        if messages.is_empty() {
            return Err(StreamError::MessagesNotFound);
        }

        Ok(messages)
    }

    pub async fn load_from_disk(id: u32) -> Result<Topic, StreamError> {
        let topic_path = format!("{}/{:0>10}", get_topics_path(), id);
        let topic_info_path = format!("{}/{}", topic_path, TOPIC_INFO);

        if !Path::new(&topic_path).exists() {
            error!("Topic directory with ID {} was not found.", id);
            return Err(StreamError::TopicNotFound);
        }

        let topic_info_file = OpenOptions::new()
            .read(true)
            .open(topic_info_path)
            .await;

        if topic_info_file.is_err() {
            error!("Failed to open topic info file for topic with ID {}.", id);
            return Err(StreamError::CannotOpenTopicInfo);
        }

        let mut topic_info = String::new();
        if topic_info_file.unwrap().read_to_string(&mut topic_info).await.is_err() {
            error!("Failed to read topic info file for topic with ID {}.", id);
            return Err(StreamError::CannotReadTopicInfo);
        }

        let dir_files = fs::read_dir(&topic_path).await;
        if dir_files.is_err() {
            error!("Failed to read directory files for topic with ID {}.", id);
            return Err(StreamError::CannotReadPartitions);
        }

        let mut topic = Topic::create(id, &topic_info, 0);
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

            let path = dir_entry.path();
            let path = path.to_str().unwrap();
            let id = dir_entry.file_name().to_str().unwrap().parse::<u32>().unwrap();
            let partition = Partition::load_from_disk(id, path).await;
            if partition.is_err() {
                continue;
            }

            let partition = partition.unwrap();
            topic.partitions.insert(partition.id, partition);
        }

        Ok(topic)
    }
}