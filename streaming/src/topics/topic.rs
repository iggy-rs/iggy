use crate::config::TopicConfig;
use crate::partitions::partition::Partition;
use crate::storage::SystemStorage;
use crate::topics::consumer_group::ConsumerGroup;
use iggy::error::Error;
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use tokio::sync::RwLock;

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
    pub(crate) current_partition_id: AtomicU32,
    pub(crate) message_expiry: Option<u32>,
}

impl Topic {
    pub fn empty(
        stream_id: u32,
        id: u32,
        topics_path: &str,
        config: Arc<TopicConfig>,
        storage: Arc<SystemStorage>,
    ) -> Topic {
        Topic::create(stream_id, id, "", 0, topics_path, config, storage, None).unwrap()
    }

    pub fn create(
        stream_id: u32,
        id: u32,
        name: &str,
        partitions_count: u32,
        topics_path: &str,
        config: Arc<TopicConfig>,
        storage: Arc<SystemStorage>,
        message_expiry: Option<u32>,
    ) -> Result<Topic, Error> {
        let path = Self::get_path(id, topics_path);
        let info_path = Self::get_info_path(&path);
        let mut topic = Topic {
            stream_id,
            id,
            name: name.to_string(),
            partitions: HashMap::new(),
            path,
            info_path,
            storage,
            consumer_groups: HashMap::new(),
            current_partition_id: AtomicU32::new(1),
            message_expiry: match message_expiry {
                Some(expiry) => Some(expiry),
                None => match config.message_expiry {
                    0 => None,
                    expiry => Some(expiry),
                },
            },
            config,
        };
        topic.add_partitions(partitions_count)?;
        Ok(topic)
    }

    pub async fn get_messages_count(&self) -> u64 {
        let mut messages_count = 0;
        for partition in self.get_partitions() {
            let partition = partition.read().await;
            messages_count += partition.get_messages_count();
        }
        messages_count
    }

    pub async fn get_size_bytes(&self) -> u64 {
        let mut size_bytes = 0;
        for partition in self.get_partitions() {
            let partition = partition.read().await;
            size_bytes += partition.get_size_bytes();
        }
        size_bytes
    }

    pub fn has_partitions(&self) -> bool {
        !self.partitions.is_empty()
    }

    pub fn get_partitions(&self) -> Vec<&RwLock<Partition>> {
        self.partitions.values().collect()
    }

    pub fn get_partitions_path(&self) -> String {
        format!("{}/{}", self.path, self.config.partition.path)
    }

    pub fn get_consumer_group_path(&self, id: u32) -> String {
        format!("{}/{}", self.get_consumer_groups_path(), id)
    }

    pub fn get_consumer_groups_path(&self) -> String {
        format!("{}/groups", self.path)
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
        let message_expiry = 10;
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
            Some(message_expiry),
        )
        .unwrap();

        assert_eq!(topic.stream_id, stream_id);
        assert_eq!(topic.id, id);
        assert_eq!(topic.path, path);
        assert_eq!(topic.info_path, info_path);
        assert_eq!(topic.name, name);
        assert_eq!(topic.partitions.len(), partitions_count as usize);
        assert_eq!(topic.message_expiry, Some(message_expiry));

        for (id, partition) in topic.partitions {
            let partition = partition.blocking_read();
            assert_eq!(partition.stream_id, stream_id);
            assert_eq!(partition.topic_id, topic.id);
            assert_eq!(partition.id, id);
            assert_eq!(partition.segments.len(), 1);
        }
    }
}
