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
        let path = format!("{}/{}", topics_path, id);
        let info_path = format!("{}/{}", path, TOPIC_INFO);

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
}
