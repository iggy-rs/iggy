use crate::config::TopicConfig;
use crate::partitions::partition::Partition;
use crate::storage::SystemStorage;
use crate::topics::consumer_group::ConsumerGroup;
use sdk::error::Error;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

pub const TOPIC_INFO: &str = "topic.info";

#[derive(Debug)]
pub struct Topic {
    pub stream_id: u32,
    pub id: u32,
    pub name: String,
    pub path: String,
    pub(crate) info_path: String,
    pub(crate) config: Arc<TopicConfig>,
    pub(crate) partitions: HashMap<u32, RwLock<Partition>>,
    pub(crate) storage: Arc<SystemStorage>,
    pub(crate) consumer_groups: HashMap<u32, RwLock<ConsumerGroup>>,
}

impl Topic {
    pub fn empty(
        stream_id: u32,
        id: u32,
        topics_path: &str,
        config: Arc<TopicConfig>,
        storage: Arc<SystemStorage>,
    ) -> Topic {
        Topic::create(stream_id, id, "", 0, topics_path, config, storage)
    }

    pub fn create(
        stream_id: u32,
        id: u32,
        name: &str,
        partitions_count: u32,
        topics_path: &str,
        config: Arc<TopicConfig>,
        storage: Arc<SystemStorage>,
    ) -> Topic {
        let path = Self::get_path(id, topics_path);
        let info_path = Self::get_info_path(&path);

        let mut topic = Topic {
            stream_id,
            id,
            name: name.to_string(),
            partitions: HashMap::new(),
            path,
            info_path,
            config: config.clone(),
            storage: storage.clone(),
            consumer_groups: HashMap::new(),
        };

        topic.partitions = (1..partitions_count + 1)
            .map(|partition_id| {
                let partition = Partition::create(
                    stream_id,
                    topic.id,
                    partition_id,
                    &topic.path,
                    true,
                    config.partition.clone(),
                    storage.clone(),
                );
                (partition_id, RwLock::new(partition))
            })
            .collect();

        topic
    }

    pub fn get_partitions(&self) -> Vec<&RwLock<Partition>> {
        self.partitions.values().collect()
    }

    pub fn create_consumer_group(&mut self, id: u32) -> Result<(), Error> {
        if self
            .consumer_groups
            .insert(
                id,
                RwLock::new(ConsumerGroup::new(
                    self.id,
                    id,
                    self.partitions.len() as u32,
                )),
            )
            .is_none()
        {
            info!(
                "Created consumer group with ID: {} for topic with ID: {} and stream with ID: {}.",
                id, self.id, self.stream_id
            );
            return Ok(());
        }

        Err(Error::ConsumerGroupAlreadyExists(id, self.id))
    }

    pub fn delete_consumer_group(&mut self, id: u32) -> Result<(), Error> {
        if self.consumer_groups.remove(&id).is_some() {
            info!(
                "Deleted consumer group with ID: {} from topic with ID: {} and stream with ID: {}.",
                id, self.id, self.stream_id
            );
            return Ok(());
        }

        Err(Error::ConsumerGroupNotFound(id, self.id))
    }

    pub fn get_consumer_group(&self, id: u32) -> Result<&RwLock<ConsumerGroup>, Error> {
        let consumer_group = self.consumer_groups.get(&id);
        if consumer_group.is_none() {
            return Err(Error::ConsumerGroupNotFound(id, self.id));
        }

        Ok(consumer_group.unwrap())
    }

    fn get_path(id: u32, topics_path: &str) -> String {
        format!("{}/{}", topics_path, id)
    }

    fn get_info_path(path: &str) -> String {
        format!("{}/{}", path, TOPIC_INFO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tests::get_test_system_storage;

    #[test]
    fn should_be_created_given_valid_parameters() {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let id = 2;
        let topics_path = "/topics";
        let name = "test";
        let partitions_count = 3;
        let config = Arc::new(TopicConfig::default());
        let path = Topic::get_path(id, topics_path);
        let info_path = Topic::get_info_path(&path);

        let topic = Topic::create(
            stream_id,
            id,
            name,
            partitions_count,
            topics_path,
            config,
            storage,
        );

        assert_eq!(topic.stream_id, stream_id);
        assert_eq!(topic.id, id);
        assert_eq!(topic.path, path);
        assert_eq!(topic.info_path, info_path);
        assert_eq!(topic.name, name);
        assert_eq!(topic.partitions.len(), partitions_count as usize);

        for (id, partition) in topic.partitions {
            let partition = partition.blocking_read();
            assert_eq!(partition.stream_id, stream_id);
            assert_eq!(partition.topic_id, topic.id);
            assert_eq!(partition.id, id);
            assert_eq!(partition.segments.len(), 1);
        }
    }
}
