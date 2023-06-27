use crate::streams::stream::Stream;
use crate::topics::topic::Topic;
use sdk::error::Error;
use tracing::info;

impl Stream {
    pub async fn create_topic(
        &mut self,
        id: u32,
        name: &str,
        partitions_count: u32,
    ) -> Result<(), Error> {
        if self.topics.contains_key(&id) {
            return Err(Error::TopicAlreadyExists(id, self.id));
        }

        let topic = Topic::create(
            self.id,
            id,
            name,
            partitions_count,
            &self.topics_path,
            self.config.topic.clone(),
            self.storage.clone(),
        );
        topic.persist().await?;
        self.topics.insert(id, topic);
        info!(
            "Created topic: {} with ID: {}, partitions: {}",
            name, id, partitions_count
        );
        Ok(())
    }

    pub fn get_topic(&self, id: u32) -> Result<&Topic, Error> {
        let topic = self.topics.get(&id);
        if topic.is_none() {
            return Err(Error::TopicNotFound(id, self.id));
        }

        Ok(topic.unwrap())
    }

    pub fn get_topic_mut(&mut self, id: u32) -> Result<&mut Topic, Error> {
        let topic = self.topics.get_mut(&id);
        if topic.is_none() {
            return Err(Error::TopicNotFound(id, self.id));
        }

        Ok(topic.unwrap())
    }

    pub fn get_topics(&self) -> Vec<&Topic> {
        self.topics.values().collect()
    }

    pub fn get_topics_mut(&mut self) -> Vec<&mut Topic> {
        self.topics.values_mut().collect()
    }

    pub async fn delete_topic(&mut self, id: u32) -> Result<(), Error> {
        let topic = self.topics.remove(&id);
        if topic.is_none() {
            return Err(Error::TopicNotFound(id, self.id));
        }

        let topic = topic.unwrap();
        if topic.delete().await.is_err() {
            return Err(Error::CannotDeleteTopic(id, self.id));
        }

        Ok(())
    }
}
