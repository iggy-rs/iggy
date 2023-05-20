use crate::config::TopicConfig;
use crate::partitions::partition::Partition;
use std::collections::HashMap;
use std::sync::Arc;

const TOPIC_INFO: &str = "topic.info";

#[derive(Debug)]
pub struct Topic {
    pub id: u32,
    pub name: String,
    pub path: String,
    pub(crate) info_path: String,
    pub(crate) config: Arc<TopicConfig>,
    pub(crate) partitions: HashMap<u32, Partition>,
}

impl Topic {
    pub fn empty(id: u32, topics_path: &str, config: Arc<TopicConfig>) -> Topic {
        Topic::create(id, topics_path, "", 0, config)
    }

    pub fn create(
        id: u32,
        topics_path: &str,
        name: &str,
        partitions_count: u32,
        config: Arc<TopicConfig>,
    ) -> Topic {
        let path = Self::get_path(id, topics_path);
        let info_path = Self::get_info_path(&path);

        let mut topic = Topic {
            id,
            name: name.to_string(),
            partitions: HashMap::new(),
            path,
            info_path,
            config: config.clone(),
        };

        topic.partitions = (1..partitions_count + 1)
            .map(|id| {
                let partition = Partition::create(id, &topic.path, true, config.partition.clone());
                (id, partition)
            })
            .collect();

        topic
    }

    pub fn get_partitions(&self) -> Vec<&Partition> {
        self.partitions.values().collect()
    }

    pub fn get_partitions_mut(&mut self) -> Vec<&mut Partition> {
        self.partitions.values_mut().collect()
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
        let id = 1;
        let topics_path = "/topics";
        let name = "test";
        let partitions_count = 3;
        let config = Arc::new(TopicConfig::default());
        let path = Topic::get_path(id, topics_path);
        let info_path = Topic::get_info_path(&path);
        
        let topic = Topic::create(1, topics_path, name, partitions_count, config);

        assert_eq!(topic.id, id);
        assert_eq!(topic.path, path);
        assert_eq!(topic.info_path, info_path);
        assert_eq!(topic.name, name);
        assert_eq!(topic.partitions.len(), partitions_count as usize);

        for (id, partition) in topic.partitions {
            assert_eq!(partition.id, id);
            assert_eq!(partition.segments.len(), 1);
        }
    }
}
