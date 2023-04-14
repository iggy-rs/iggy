use crate::error::Error;
use crate::streams::stream::Stream;
use crate::topics::topic::Topic;
use tracing::info;

impl Stream {
    pub async fn create_topic(
        &mut self,
        id: u32,
        name: &str,
        partitions: u32,
    ) -> Result<(), Error> {
        let mut topic = Topic::create(
            id,
            &self.topics_path,
            name,
            partitions,
            self.config.topic.clone(),
        );
        topic.persist().await?;
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

    pub async fn delete_topic(&mut self, id: u32) -> Result<(), Error> {
        let topic = self.topics.remove(&id);
        if topic.is_none() {
            return Err(Error::TopicNotFound(id));
        }

        let topic = topic.unwrap();
        if topic.delete().await.is_err() {
            return Err(Error::CannotDeleteTopic(id));
        }

        info!("Deleted topic: {:?} with ID: {:?}", topic.name, id);
        Ok(())
    }
}
