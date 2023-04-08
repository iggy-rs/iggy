use crate::message::Message;
use crate::stream_error::StreamError;
use crate::topic::Topic;
use crate::{get_topics_path, topic};
use std::collections::HashMap;
use tracing::{error, info};

#[derive(Debug)]
pub struct Stream {
    pub topics: HashMap<u32, Topic>,
}

impl Stream {
    pub fn create() -> Self {
        Stream {
            topics: HashMap::new(),
        }
    }

    pub async fn create_topic(
        &mut self,
        id: u32,
        name: &str,
        partitions: u32,
    ) -> Result<(), StreamError> {
        let topic = topic::Topic::create(id, name, partitions);
        topic.save_on_disk().await?;
        self.topics.insert(id, topic);
        info!(
            "Created topic: {:?} with ID: {:?}, partitions: {:?}",
            name, id, partitions
        );
        Ok(())
    }

    pub fn get_topics(&self) -> Vec<&Topic> {
        self.topics.values().collect()
    }

    pub fn get_topics_mut(&mut self) -> Vec<&mut Topic> {
        self.topics.values_mut().collect()
    }

    pub async fn send_message(
        &mut self,
        topic_id: u32,
        partition_id: u32,
        message: Message,
    ) -> Result<(), StreamError> {
        let topic = self.topics.get_mut(&topic_id);
        if topic.is_none() {
            return Err(StreamError::TopicNotFound(topic_id));
        }

        let topic = topic.unwrap();
        topic.send_message(partition_id, message).await?;
        Ok(())
    }

    pub async fn delete_topic(&mut self, id: u32) -> Result<(), StreamError> {
        let topic = self.topics.remove(&id);
        if topic.is_none() {
            return Err(StreamError::TopicNotFound(id));
        }

        let topic = topic.unwrap();
        if topic.delete().await.is_err() {
            return Err(StreamError::CannotDeleteTopic(id));
        }

        info!("Deleted topic: {:?} with ID: {:?}", topic.name, id);
        Ok(())
    }

    pub fn get_messages(
        &mut self,
        topic_id: u32,
        partition_id: u32,
        offset: u64,
        count: u32,
    ) -> Result<Vec<&Message>, StreamError> {
        let topic = self.topics.get_mut(&topic_id);
        if topic.is_none() {
            return Err(StreamError::TopicNotFound(topic_id));
        }

        topic.unwrap().get_messages(partition_id, offset, count)
    }

    pub async fn load_topics_from_disk(&mut self) {
        info!("Loading topics from disk...");
        let topics_path = &get_topics_path();
        let topics = std::fs::read_dir(topics_path).unwrap();
        for topic in topics {
            info!("Trying to load topic from disk...");
            let topic = topic.unwrap();
            let topic_id = topic
                .file_name()
                .into_string()
                .unwrap()
                .parse::<u32>()
                .unwrap();
            info!("Loading topic with ID: {} from disk...", topic_id);
            let topic = Topic::load_from_disk(topic_id).await;
            if topic.is_err() {
                error!("{}", topic.err().unwrap());
                continue;
            }

            let topic = topic.unwrap();
            info!("Loaded topic with ID: {} from disk.", topic_id);
            self.topics.insert(topic_id, topic);
        }

        info!("Loaded {} topic(s) from disk.", self.topics.len());
    }
}
