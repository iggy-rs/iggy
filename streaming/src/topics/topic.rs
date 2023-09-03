use crate::config::SystemConfig;
use crate::partitions::partition::Partition;
use crate::storage::SystemStorage;
use crate::topics::consumer_group::ConsumerGroup;
use iggy::error::Error;
use iggy::utils::timestamp;
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct Topic {
    pub stream_id: u32,
    pub topic_id: u32,
    pub name: String,
    pub path: String,
    pub partitions_path: String,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) partitions: HashMap<u32, RwLock<Partition>>,
    pub(crate) storage: Arc<SystemStorage>,
    pub(crate) consumer_groups: HashMap<u32, RwLock<ConsumerGroup>>,
    pub(crate) current_partition_id: AtomicU32,
    pub message_expiry: Option<u32>,
    pub created_at: u64,
}

impl Topic {
    pub fn empty(
        stream_id: u32,
        topic_id: u32,
        config: Arc<SystemConfig>,
        storage: Arc<SystemStorage>,
    ) -> Topic {
        Topic::create(stream_id, topic_id, "", 0, config, storage, None).unwrap()
    }

    pub fn create(
        stream_id: u32,
        topic_id: u32,
        name: &str,
        partitions_count: u32,
        config: Arc<SystemConfig>,
        storage: Arc<SystemStorage>,
        message_expiry: Option<u32>,
    ) -> Result<Topic, Error> {
        let path = config.get_topic_path(stream_id, topic_id);
        let partitions_path = config.get_partitions_path(stream_id, topic_id);
        let mut topic = Topic {
            stream_id,
            topic_id,
            name: name.to_string(),
            partitions: HashMap::new(),
            path,
            partitions_path,
            storage,
            consumer_groups: HashMap::new(),
            current_partition_id: AtomicU32::new(1),
            message_expiry: match message_expiry {
                Some(expiry) => match expiry {
                    0 => None,
                    _ => Some(expiry),
                },
                None => match config.segment.message_expiry {
                    0 => None,
                    expiry => Some(expiry),
                },
            },
            config,
            created_at: timestamp::get(),
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

    pub fn get_partition(&self, partition_id: u32) -> Result<&RwLock<Partition>, Error> {
        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(Error::PartitionNotFound(
                partition_id,
                self.topic_id,
                self.stream_id,
            ));
        }

        Ok(partition.unwrap())
    }

    pub fn get_consumer_group_path(&self, id: u32) -> String {
        format!("{}/{}", self.get_consumer_groups_path(), id)
    }

    pub fn get_consumer_groups_path(&self) -> String {
        format!("{}/groups", self.path)
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
        let topic_id = 2;
        let name = "test";
        let partitions_count = 3;
        let message_expiry = 10;
        let config = Arc::new(SystemConfig::default());
        let path = config.get_topic_path(stream_id, topic_id);

        let topic = Topic::create(
            stream_id,
            topic_id,
            name,
            partitions_count,
            config,
            storage,
            Some(message_expiry),
        )
        .unwrap();

        assert_eq!(topic.stream_id, stream_id);
        assert_eq!(topic.topic_id, topic_id);
        assert_eq!(topic.path, path);
        assert_eq!(topic.name, name);
        assert_eq!(topic.partitions.len(), partitions_count as usize);
        assert_eq!(topic.message_expiry, Some(message_expiry));

        for (id, partition) in topic.partitions {
            let partition = partition.blocking_read();
            assert_eq!(partition.stream_id, stream_id);
            assert_eq!(partition.topic_id, topic.topic_id);
            assert_eq!(partition.partition_id, id);
            assert_eq!(partition.segments.len(), 1);
        }
    }
}
