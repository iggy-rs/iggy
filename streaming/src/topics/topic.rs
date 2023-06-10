use crate::config::TopicConfig;
use crate::partitions::partition::Partition;
use std::collections::HashMap;
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
}

impl Topic {
    pub fn empty(stream_id: u32, id: u32, topics_path: &str, config: Arc<TopicConfig>) -> Topic {
        Topic::create(stream_id, id, "", 0, topics_path, config)
    }

    pub fn create(
        stream_id: u32,
        id: u32,
        name: &str,
        partitions_count: u32,
        topics_path: &str,
        config: Arc<TopicConfig>,
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
                );
                (partition_id, RwLock::new(partition))
            })
            .collect();

        topic
    }

    pub fn get_partitions(&self) -> Vec<&RwLock<Partition>> {
        self.partitions.values().collect()
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

    #[test]
    fn should_be_created_given_valid_parameters() {
        let stream_id = 1;
        let id = 2;
        let topics_path = "/topics";
        let name = "test";
        let partitions_count = 3;
        let config = Arc::new(TopicConfig::default());
        let path = Topic::get_path(id, topics_path);
        let info_path = Topic::get_info_path(&path);

        let topic = Topic::create(stream_id, id, name, partitions_count, topics_path, config);

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
