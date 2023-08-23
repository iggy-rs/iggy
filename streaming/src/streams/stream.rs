use crate::config::SystemConfig;
use crate::storage::SystemStorage;
use crate::topics::topic::Topic;
use std::collections::HashMap;
use std::sync::Arc;

pub const STREAM_INFO: &str = "stream.info";

#[derive(Debug)]
pub struct Stream {
    pub id: u32,
    pub name: String,
    pub path: String,
    pub topics_path: String,
    pub info_path: String,
    pub(crate) topics: HashMap<u32, Topic>,
    pub(crate) topics_ids: HashMap<String, u32>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) storage: Arc<SystemStorage>,
}

impl Stream {
    pub fn empty(
        id: u32,
        streams_path: &str,
        config: Arc<SystemConfig>,
        storage: Arc<SystemStorage>,
    ) -> Self {
        Stream::create(id, "", streams_path, config, storage)
    }

    pub fn create(
        id: u32,
        name: &str,
        streams_path: &str,
        config: Arc<SystemConfig>,
        storage: Arc<SystemStorage>,
    ) -> Self {
        let path = Self::get_path(id, streams_path);
        let info_path = Self::get_info_path(&path);
        let topics_path = Self::get_topics_path(&path, &config.topic.path);

        Stream {
            id,
            name: name.to_string(),
            path,
            topics_path,
            info_path,
            config,
            topics: HashMap::new(),
            topics_ids: HashMap::new(),
            storage,
        }
    }

    pub async fn get_messages_count(&self) -> u64 {
        let mut messages_count = 0;
        for topic in self.topics.values() {
            messages_count += topic.get_messages_count().await;
        }
        messages_count
    }

    pub async fn get_size_bytes(&self) -> u64 {
        let mut size_bytes = 0;
        for topic in self.topics.values() {
            size_bytes += topic.get_size_bytes().await;
        }
        size_bytes
    }

    fn get_path(id: u32, streams_path: &str) -> String {
        format!("{}/{}", streams_path, id)
    }

    fn get_info_path(path: &str) -> String {
        format!("{}/{}", path, STREAM_INFO)
    }

    fn get_topics_path(path: &str, topics_path: &str) -> String {
        format!("{}/{}", path, topics_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tests::get_test_system_storage;

    #[test]
    fn should_be_created_given_valid_parameters() {
        let storage = Arc::new(get_test_system_storage());
        let id = 1;
        let name = "test";
        let streams_path = "/streams";
        let config = Arc::new(SystemConfig::default());
        let path = Stream::get_path(id, streams_path);
        let info_path = Stream::get_info_path(&path);
        let topics_path = Stream::get_topics_path(&path, &config.topic.path);

        let stream = Stream::create(id, name, streams_path, config, storage);

        assert_eq!(stream.id, id);
        assert_eq!(stream.name, name);
        assert_eq!(stream.path, path);
        assert_eq!(stream.info_path, info_path);
        assert_eq!(stream.topics_path, topics_path);
        assert!(stream.topics.is_empty());
    }
}
