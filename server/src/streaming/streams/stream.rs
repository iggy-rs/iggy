use crate::configs::system::SystemConfig;
use crate::streaming::storage::SystemStorage;
use crate::streaming::topics::topic::Topic;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct Stream {
    pub stream_id: u32,
    pub name: String,
    pub path: String,
    pub topics_path: String,
    pub created_at: IggyTimestamp,
    pub current_topic_id: AtomicU32,
    pub size_bytes: Arc<AtomicU64>,
    pub messages_count: Arc<AtomicU64>,
    pub segments_count: Arc<AtomicU32>,
    pub(crate) topics: HashMap<u32, Topic>,
    pub(crate) topics_ids: HashMap<String, u32>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) storage: Arc<SystemStorage>,
}

impl Stream {
    pub fn empty(
        id: u32,
        name: &str,
        config: Arc<SystemConfig>,
        storage: Arc<SystemStorage>,
    ) -> Self {
        Stream::create(id, name, config, storage)
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
            stream_id: id,
            name: name.to_string(),
            path,
            topics_path,
            config,
            current_topic_id: AtomicU32::new(1),
            size_bytes: Arc::new(AtomicU64::new(0)),
            messages_count: Arc::new(AtomicU64::new(0)),
            segments_count: Arc::new(AtomicU32::new(0)),
            topics: HashMap::new(),
            topics_ids: HashMap::new(),
            storage,
            created_at: IggyTimestamp::now(),
        }
    }

    pub fn get_size(&self) -> IggyByteSize {
        IggyByteSize::from(self.size_bytes.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::storage::tests::get_test_system_storage;

    #[test]
    fn should_be_created_given_valid_parameters() {
        let storage = Arc::new(get_test_system_storage());
        let id = 1;
        let name = "test";
        let config = Arc::new(SystemConfig::default());
        let path = config.get_stream_path(id);
        let topics_path = config.get_topics_path(id);

        let stream = Stream::create(id, name, config, storage);

        assert_eq!(stream.stream_id, id);
        assert_eq!(stream.name, name);
        assert_eq!(stream.path, path);
        assert_eq!(stream.topics_path, topics_path);
        assert!(stream.topics.is_empty());
    }
}
