use crate::streams::stream::Stream;
use crate::topics::topic::Topic;
use crate::utils::text;
use iggy::error::Error;
use iggy::identifier::{IdKind, Identifier};
use tracing::info;

impl Stream {
    pub async fn create_topic(
        &mut self,
        id: u32,
        name: &str,
        partitions_count: u32,
    ) -> Result<(), Error> {
        if self.topics.contains_key(&id) {
            return Err(Error::TopicIdAlreadyExists(id, self.id));
        }

        let name = text::to_lowercase_non_whitespace(name);
        if self.topics_ids.contains_key(&name) {
            return Err(Error::TopicNameAlreadyExists(name, self.id));
        }

        let topic = Topic::create(
            self.id,
            id,
            &name,
            partitions_count,
            &self.topics_path,
            self.config.topic.clone(),
            self.storage.clone(),
        )?;
        topic.persist().await?;
        info!(
            "Created topic: {} with ID: {}, partitions: {}",
            name, id, partitions_count
        );
        self.topics_ids.insert(name, id);
        self.topics.insert(id, topic);

        Ok(())
    }

    pub fn get_topic(&self, identifier: &Identifier) -> Result<&Topic, Error> {
        match identifier.kind {
            IdKind::Numeric => self.get_topic_by_id(identifier.get_u32_value().unwrap()),
            IdKind::String => self.get_topic_by_name(&identifier.get_string_value().unwrap()),
        }
    }

    pub fn get_topic_mut(&mut self, identifier: &Identifier) -> Result<&mut Topic, Error> {
        match identifier.kind {
            IdKind::Numeric => self.get_topic_by_id_mut(identifier.get_u32_value().unwrap()),
            IdKind::String => self.get_topic_by_name_mut(&identifier.get_string_value().unwrap()),
        }
    }

    fn get_topic_by_id(&self, id: u32) -> Result<&Topic, Error> {
        let topic = self.topics.get(&id);
        if topic.is_none() {
            return Err(Error::TopicIdNotFound(id, self.id));
        }

        Ok(topic.unwrap())
    }

    fn get_topic_by_name(&self, name: &str) -> Result<&Topic, Error> {
        let topic_id = self.topics_ids.get(name);
        if topic_id.is_none() {
            return Err(Error::TopicNameNotFound(name.to_string(), self.id));
        }

        self.get_topic_by_id(*topic_id.unwrap())
    }

    fn get_topic_by_id_mut(&mut self, id: u32) -> Result<&mut Topic, Error> {
        let topic = self.topics.get_mut(&id);
        if topic.is_none() {
            return Err(Error::TopicIdNotFound(id, self.id));
        }

        Ok(topic.unwrap())
    }

    fn get_topic_by_name_mut(&mut self, name: &str) -> Result<&mut Topic, Error> {
        let topic_id = self.topics_ids.get(name);
        if topic_id.is_none() {
            return Err(Error::TopicNameNotFound(name.to_string(), self.id));
        }

        Ok(self.topics.get_mut(topic_id.unwrap()).unwrap())
    }

    pub fn get_topics(&self) -> Vec<&Topic> {
        self.topics.values().collect()
    }

    pub fn get_topics_mut(&mut self) -> Vec<&mut Topic> {
        self.topics.values_mut().collect()
    }

    pub async fn delete_topic(&mut self, id: &Identifier) -> Result<Topic, Error> {
        let topic = match id.kind {
            IdKind::Numeric => {
                let topic_id = id.get_u32_value().unwrap();
                let topic = self.topics.remove(&topic_id);
                if topic.is_none() {
                    return Err(Error::TopicIdNotFound(topic_id, self.id));
                }

                let topic = topic.unwrap();
                self.topics_ids.remove(&topic.name);
                topic
            }
            IdKind::String => {
                let topic_name = id.get_string_value().unwrap();
                let topic_id = self.topics_ids.remove(&topic_name);
                if topic_id.is_none() {
                    return Err(Error::TopicNameNotFound(topic_name, self.id));
                }

                let topic_id = topic_id.unwrap();
                let topic = self.topics.remove(&topic_id);
                topic.unwrap()
            }
        };

        if topic.delete().await.is_err() {
            return Err(Error::CannotDeleteTopic(topic.id, self.id));
        }

        Ok(topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StreamConfig;
    use crate::storage::tests::get_test_system_storage;
    use std::sync::Arc;

    #[tokio::test]
    async fn should_get_topic_by_id_and_name() {
        let stream_id = 1;
        let stream_name = "test_stream";
        let topic_id = 2;
        let topic_name = "test_topic";
        let path = "/stream";
        let config = Arc::new(StreamConfig::default());
        let storage = Arc::new(get_test_system_storage());
        let mut stream = Stream::create(stream_id, stream_name, path, config, storage);
        stream.create_topic(topic_id, topic_name, 1).await.unwrap();

        let topic = stream.get_topic(&Identifier::numeric(topic_id).unwrap());
        assert!(topic.is_ok());
        let topic = topic.unwrap();
        assert_eq!(topic.id, topic_id);
        assert_eq!(topic.name, topic_name);

        let topic = stream.get_topic(&Identifier::string(topic_name).unwrap());
        assert!(topic.is_ok());
        let topic = topic.unwrap();
        assert_eq!(topic.id, topic_id);
        assert_eq!(topic.name, topic_name);
    }
}
