use crate::configs::system::SystemConfig;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::storage::SystemStorage;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use core::fmt;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
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
    pub(crate) compression_algorithm: CompressionAlgorithm,
    pub(crate) partitions: HashMap<u32, Arc<RwLock<Partition>>>,
    pub(crate) storage: Arc<SystemStorage>,
    pub(crate) consumer_groups: HashMap<u32, RwLock<ConsumerGroup>>,
    pub(crate) consumer_groups_ids: HashMap<String, u32>,
    pub(crate) current_partition_id: AtomicU32,
    pub message_expiry: Option<u32>,
    pub max_topic_size: Option<IggyByteSize>,
    pub replication_factor: u8,
    pub created_at: u64,
}

impl Topic {
    pub fn empty(
        stream_id: u32,
        topic_id: u32,
        config: Arc<SystemConfig>,
        storage: Arc<SystemStorage>,
    ) -> Topic {
        Topic::create(
            stream_id,
            topic_id,
            "",
            0,
            config,
            CompressionAlgorithm::None,
            storage,
            None,
            None,
            1,
        )
        .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        stream_id: u32,
        topic_id: u32,
        name: &str,
        partitions_count: u32,
        config: Arc<SystemConfig>,
        compression_algorithm: CompressionAlgorithm,
        storage: Arc<SystemStorage>,
        message_expiry: Option<u32>,
        max_topic_size: Option<IggyByteSize>,
        replication_factor: u8,
    ) -> Result<Topic, IggyError> {
        let path = config.get_topic_path(stream_id, topic_id);
        let partitions_path = config.get_partitions_path(stream_id, topic_id);
        let mut topic: Topic = Topic {
            stream_id,
            topic_id,
            name: name.to_string(),
            partitions: HashMap::new(),
            path,
            partitions_path,
            compression_algorithm,
            storage,
            consumer_groups: HashMap::new(),
            consumer_groups_ids: HashMap::new(),
            current_partition_id: AtomicU32::new(1),
            message_expiry: match message_expiry {
                Some(expiry) => match expiry {
                    0 => None,
                    _ => Some(expiry),
                },
                None => match config.retention_policy.message_expiry.as_secs() {
                    0 => None,
                    expiry => Some(expiry),
                },
            },
            max_topic_size,
            replication_factor,
            config,
            created_at: IggyTimestamp::now().to_micros(),
        };

        topic.add_partitions(partitions_count)?;
        Ok(topic)
    }

    pub async fn get_size(&self) -> IggyByteSize {
        let mut size_bytes = 0;
        for partition in self.get_partitions() {
            let partition = partition.read().await;
            size_bytes += partition.get_size_bytes();
        }
        IggyByteSize::from(size_bytes)
    }

    pub fn get_partitions(&self) -> Vec<Arc<RwLock<Partition>>> {
        self.partitions.values().map(Arc::clone).collect()
    }

    pub fn get_partition(&self, partition_id: u32) -> Result<Arc<RwLock<Partition>>, IggyError> {
        match self.partitions.get(&partition_id) {
            Some(partition_arc) => Ok(partition_arc.clone()),
            None => Err(IggyError::PartitionNotFound(
                partition_id,
                self.topic_id,
                self.stream_id,
            )),
        }
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        let max_topic_size = match self.max_topic_size {
            Some(size) => size.as_human_string_with_zero_as_unlimited(),
            None => "unlimited".to_owned(),
        };
        write!(f, "ID: {}, ", self.topic_id)?;
        write!(f, "stream ID: {}, ", self.stream_id)?;
        write!(f, "name: {}, ", self.name)?;
        write!(f, "path: {}, ", self.path)?;
        write!(f, "partitions count: {:?}, ", self.partitions.len())?;
        write!(f, "message expiry (s): {:?}, ", self.message_expiry)?;
        write!(f, "max topic size (B): {:?}, ", max_topic_size)?;
        write!(f, "replication factor: {}, ", self.replication_factor)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::streaming::storage::tests::get_test_system_storage;

    #[test]
    fn should_be_created_given_valid_parameters() {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let name = "test";
        let partitions_count = 3;
        let message_expiry = 10;
        let max_topic_size = IggyByteSize::from_str("2 GB").unwrap();
        let replication_factor = 1;
        let config = Arc::new(SystemConfig::default());
        let path = config.get_topic_path(stream_id, topic_id);

        let topic = Topic::create(
            stream_id,
            topic_id,
            name,
            partitions_count,
            config,
            CompressionAlgorithm::None,
            storage,
            Some(message_expiry),
            Some(max_topic_size),
            replication_factor,
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
