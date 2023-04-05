use std::collections::HashMap;
use tracing::info;
use crate::message::Message;
use crate::stream_error::StreamError;
use crate::topic;
use crate::topic::Topic;

#[derive(Debug)]
pub struct Stream {
    pub topics: HashMap<u32, Topic>
}

impl Stream {
    pub fn create() -> Self {
        Stream {
            topics: HashMap::new()
        }
    }

    pub async fn create_topic(&mut self, id: u32, name: &str, partitions: u32) -> Result<(), StreamError> {
        let topic = topic::Topic::create(id, name, partitions);
        topic.save_on_disk().await?;
        self.topics.insert(id, topic);
        info!("Created topic: {:?} with ID: {:?}, partitions: {:?}", name, id, partitions);
        Ok(())
    }

    pub async fn delete_topic(&mut self, id: u32) -> Result<(), StreamError> {
        let topic = self.topics.remove(&id);
        if topic.is_none() {
            return Err(StreamError::TopicNotFound);
        }

        let topic = topic.unwrap();
        if topic.delete().await.is_err() {
            return Err(StreamError::CannotDeleteTopic);
        }

        info!("Deleted topic: {:?} with ID: {:?}", topic.name, id);
        Ok(())
    }

    pub fn get_messages(&mut self, topic_id: u32, partition_id: u32, offset: u64, count: u32) -> Result<Vec<&Message>, StreamError> {
        let topic = self.topics.get_mut(&topic_id);
        if topic.is_none() {
            return Err(StreamError::TopicNotFound);
        }

        topic.unwrap().get_messages(partition_id, offset, count)
    }
}
