use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::topic::Topic;
use iggy::error::IggyError;
use iggy::identifier::{IdKind, Identifier};
use iggy::locking::IggySharedMutFn;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::text;
use std::sync::atomic::Ordering;
use tracing::{debug, info};

impl Stream {
    pub fn get_topics_count(&self) -> u32 {
        self.topics.len() as u32
    }

    pub async fn create_topic(
        &mut self,
        topic_id: Option<u32>,
        name: &str,
        partitions_count: u32,
        message_expiry: Option<u32>,
        max_topic_size: Option<IggyByteSize>,
        replication_factor: u8,
    ) -> Result<(), IggyError> {
        let name = text::to_lowercase_non_whitespace(name);
        if self.topics_ids.contains_key(&name) {
            return Err(IggyError::TopicNameAlreadyExists(name, self.stream_id));
        }

        let mut id;
        if topic_id.is_none() {
            id = self.current_topic_id.fetch_add(1, Ordering::SeqCst);
            loop {
                if self.topics.contains_key(&id) {
                    if id == u32::MAX {
                        return Err(IggyError::TopicIdAlreadyExists(id, self.stream_id));
                    }
                    id = self.current_topic_id.fetch_add(1, Ordering::SeqCst);
                } else {
                    break;
                }
            }
        } else {
            id = topic_id.unwrap();
        }

        if self.topics.contains_key(&id) {
            return Err(IggyError::TopicIdAlreadyExists(id, self.stream_id));
        }

        // TODO: check if max_topic_size is not lower than system.segment.size
        let topic = Topic::create(
            self.stream_id,
            id,
            &name,
            partitions_count,
            self.config.clone(),
            self.storage.clone(),
            self.size_bytes.clone(),
            self.messages_count.clone(),
            message_expiry,
            max_topic_size,
            replication_factor,
        )?;
        topic.persist().await?;
        info!("Created topic {}", topic);
        self.topics_ids.insert(name, id);
        self.topics.insert(id, topic);

        Ok(())
    }

    pub async fn update_topic(
        &mut self,
        id: &Identifier,
        name: &str,
        message_expiry: Option<u32>,
        max_topic_size: Option<IggyByteSize>,
        replication_factor: u8,
    ) -> Result<(), IggyError> {
        let topic_id;
        {
            let topic = self.get_topic(id)?;
            topic_id = topic.topic_id;
        }

        let updated_name = text::to_lowercase_non_whitespace(name);

        {
            if let Some(topic_id_by_name) = self.topics_ids.get(&updated_name) {
                if *topic_id_by_name != topic_id {
                    return Err(IggyError::TopicNameAlreadyExists(
                        updated_name.to_string(),
                        self.stream_id,
                    ));
                }
            }
        }

        let old_topic_name = {
            let topic = self.get_topic(id)?;
            topic.name.clone()
        };

        {
            self.topics_ids.remove(&old_topic_name.clone());
            self.topics_ids.insert(updated_name.clone(), topic_id);
            let topic = self.get_topic_mut(id)?;
            topic.name = updated_name;
            topic.message_expiry = message_expiry;
            for partition in topic.partitions.values_mut() {
                let mut partition = partition.write().await;
                partition.message_expiry = message_expiry;
                for segment in partition.segments.iter_mut() {
                    segment.message_expiry = message_expiry;
                }
            }
            topic.max_topic_size = max_topic_size;
            topic.replication_factor = replication_factor;

            topic.persist().await?;
            info!("Updated topic: {topic}");
        }

        Ok(())
    }

    pub fn remove_topic(&mut self, identifier: &Identifier) -> Result<Topic, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.remove_topic_by_id(identifier.get_u32_value()?),
            IdKind::String => self.remove_topic_by_name(&identifier.get_cow_str_value()?),
        }
    }

    pub fn get_topics(&self) -> Vec<&Topic> {
        self.topics.values().collect()
    }

    pub fn get_topic(&self, identifier: &Identifier) -> Result<&Topic, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_topic_by_id(identifier.get_u32_value()?),
            IdKind::String => self.get_topic_by_name(&identifier.get_cow_str_value()?),
        }
    }

    pub fn get_topic_mut(&mut self, identifier: &Identifier) -> Result<&mut Topic, IggyError> {
        match identifier.kind {
            IdKind::Numeric => self.get_topic_by_id_mut(identifier.get_u32_value()?),
            IdKind::String => self.get_topic_by_name_mut(&identifier.get_cow_str_value()?),
        }
    }

    fn get_topic_by_id(&self, id: u32) -> Result<&Topic, IggyError> {
        self.topics
            .get(&id)
            .ok_or(IggyError::TopicIdNotFound(id, self.stream_id))
    }

    fn get_topic_by_name(&self, name: &str) -> Result<&Topic, IggyError> {
        self.topics_ids
            .get(name)
            .map(|topic_id| self.get_topic_by_id(*topic_id))
            .ok_or_else(|| IggyError::TopicNameNotFound(name.to_string(), self.stream_id))?
    }

    fn get_topic_by_id_mut(&mut self, id: u32) -> Result<&mut Topic, IggyError> {
        self.topics
            .get_mut(&id)
            .ok_or(IggyError::TopicIdNotFound(id, self.stream_id))
    }

    fn get_topic_by_name_mut(&mut self, name: &str) -> Result<&mut Topic, IggyError> {
        self.topics_ids
            .get(name)
            .and_then(|topic_id| self.topics.get_mut(topic_id))
            .ok_or_else(|| IggyError::TopicNameNotFound(name.to_string(), self.stream_id))
    }

    fn remove_topic_by_id(&mut self, id: u32) -> Result<Topic, IggyError> {
        let topic = self
            .topics
            .remove(&id)
            .ok_or_else(|| IggyError::TopicIdNotFound(id, self.stream_id))?;

        self.topics_ids
            .remove(&topic.name)
            .ok_or_else(|| IggyError::TopicNameNotFound(topic.name.clone(), self.stream_id))?;
        Ok(topic)
    }

    fn remove_topic_by_name(&mut self, name: &str) -> Result<Topic, IggyError> {
        let topic_id = self
            .topics_ids
            .remove(name)
            .ok_or_else(|| IggyError::TopicNameNotFound(name.to_owned(), self.stream_id))?;

        self.topics
            .remove(&topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id, self.stream_id))
    }

    pub async fn delete_topic(&mut self, id: &Identifier) -> Result<Topic, IggyError> {
        let topic = self.remove_topic(id)?;
        let topic_id = topic.topic_id;
        let current_topic_id = self.current_topic_id.load(Ordering::SeqCst);
        if current_topic_id > topic_id {
            self.current_topic_id.store(topic_id, Ordering::SeqCst);
        }

        topic.delete().await.map_err(|err| {
            debug!("Delete topic failed: {}", err);
            IggyError::CannotDeleteTopic(topic.topic_id, self.stream_id)
        })?;
        Ok(topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::SystemConfig;
    use crate::streaming::storage::tests::get_test_system_storage;
    use std::sync::Arc;

    #[tokio::test]
    async fn should_get_topic_by_id_and_name() {
        let stream_id = 1;
        let stream_name = "test_stream";
        let topic_id = 2;
        let topic_name = "test_topic";
        let message_expiry = Some(10);
        let max_topic_size = Some(IggyByteSize::from(100));
        let config = Arc::new(SystemConfig::default());
        let storage = Arc::new(get_test_system_storage());
        let mut stream = Stream::create(stream_id, stream_name, config, storage);
        stream
            .create_topic(
                Some(topic_id),
                topic_name,
                1,
                message_expiry,
                max_topic_size,
                1,
            )
            .await
            .unwrap();

        let topic = stream.get_topic(&Identifier::numeric(topic_id).unwrap());
        assert!(topic.is_ok());
        let topic = topic.unwrap();
        assert_eq!(topic.topic_id, topic_id);
        assert_eq!(topic.name, topic_name);

        let topic = stream.get_topic(&Identifier::named(topic_name).unwrap());
        assert!(topic.is_ok());
        let topic = topic.unwrap();
        assert_eq!(topic.topic_id, topic_id);
        assert_eq!(topic.name, topic_name);
    }
}
