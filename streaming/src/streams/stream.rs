use crate::config::SystemConfig;
use crate::storage::SystemStorage;
use crate::topics::topic::Topic;
use iggy::utils::timestamp;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct Stream {
    pub id: u32,
    pub name: String,
    pub path: String,
    pub topics_path: String,
    pub created_at: u64,
    pub(crate) topics: HashMap<u32, Topic>,
    pub(crate) topics_ids: HashMap<String, u32>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) storage: Arc<SystemStorage>,
}

impl Stream {
    pub fn empty(id: u32, config: Arc<SystemConfig>, storage: Arc<SystemStorage>) -> Self {
        Stream::create(id, "", config, storage)
    }

    pub fn create(
        id: u32,
        name: &str,
        config: Arc<SystemConfig>,
        storage: Arc<SystemStorage>,
    ) -> Self {
        let path = config.get_stream_path(id);
        let topics_path = config.get_topics_path(id);

        Stream {
            id,
            name: name.to_string(),
            path,
            topics_path,
            config,
            topics: HashMap::new(),
            topics_ids: HashMap::new(),
            storage,
            created_at: timestamp::get(),
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
        let config = Arc::new(SystemConfig::default());
        let path = config.get_stream_path(id);
        let topics_path = config.get_topics_path(id);

        let stream = Stream::create(id, name, config, storage);

        assert_eq!(stream.id, id);
        assert_eq!(stream.name, name);
        assert_eq!(stream.path, path);
        assert_eq!(stream.topics_path, topics_path);
        assert!(stream.topics.is_empty());
    }
}
