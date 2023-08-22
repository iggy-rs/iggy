use crate::config::SegmentConfig;
use crate::segments::index::Index;
use crate::segments::time_index::TimeIndex;
use crate::storage::SystemStorage;
use iggy::models::message::Message;
use std::sync::Arc;

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const TIME_INDEX_EXTENSION: &str = "timeindex";
pub const MAX_SIZE_BYTES: u32 = 1024 * 1024 * 1024;

#[derive(Debug)]
pub struct Segment {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub current_offset: u64,
    pub index_path: String,
    pub log_path: String,
    pub time_index_path: String,
    pub current_size_bytes: u32,
    pub is_closed: bool,
    pub(crate) unsaved_messages: Option<Vec<Arc<Message>>>,
    pub(crate) config: Arc<SegmentConfig>,
    pub(crate) indexes: Option<Vec<Index>>,
    pub(crate) time_indexes: Option<Vec<TimeIndex>>,
    pub(crate) storage: Arc<SystemStorage>,
}

impl Segment {
    pub fn create(
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        start_offset: u64,
        partition_path: &str,
        config: Arc<SegmentConfig>,
        storage: Arc<SystemStorage>,
    ) -> Segment {
        let path = Self::get_path(partition_path, start_offset);

        Segment {
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            end_offset: 0,
            current_offset: start_offset,
            log_path: Self::get_log_path(&path),
            index_path: Self::get_index_path(&path),
            time_index_path: Self::get_time_index_path(&path),
            current_size_bytes: 0,
            indexes: match config.cache_indexes {
                true => Some(Vec::new()),
                false => None,
            },
            time_indexes: match config.cache_time_indexes {
                true => Some(Vec::new()),
                false => None,
            },
            unsaved_messages: None,
            is_closed: false,
            config,
            storage,
        }
    }

    pub fn is_full(&self) -> bool {
        self.current_size_bytes >= self.config.size_bytes
    }

    fn get_path(partition_path: &str, start_offset: u64) -> String {
        format!("{}/{:0>20}", partition_path, start_offset)
    }

    fn get_log_path(path: &str) -> String {
        format!("{}.{}", path, LOG_EXTENSION)
    }

    fn get_index_path(path: &str) -> String {
        format!("{}.{}", path, INDEX_EXTENSION)
    }

    fn get_time_index_path(path: &str) -> String {
        format!("{}.{}", path, TIME_INDEX_EXTENSION)
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
        let partition_id = 3;
        let partition_path = "/topics/2/3";
        let start_offset = 0;
        let config = Arc::new(SegmentConfig::default());
        let path = Segment::get_path(partition_path, start_offset);
        let log_path = Segment::get_log_path(&path);
        let index_path = Segment::get_index_path(&path);
        let time_index_path = Segment::get_time_index_path(&path);

        let segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            partition_path,
            config,
            storage,
        );

        assert_eq!(segment.stream_id, stream_id);
        assert_eq!(segment.topic_id, topic_id);
        assert_eq!(segment.partition_id, partition_id);
        assert_eq!(segment.start_offset, start_offset);
        assert_eq!(segment.current_offset, 0);
        assert_eq!(segment.end_offset, 0);
        assert_eq!(segment.current_size_bytes, 0);
        assert_eq!(segment.log_path, log_path);
        assert_eq!(segment.index_path, index_path);
        assert_eq!(segment.time_index_path, time_index_path);
        assert!(segment.unsaved_messages.is_none());
        assert!(segment.indexes.is_some());
        assert!(segment.time_indexes.is_some());
        assert!(!segment.is_closed);
        assert!(!segment.is_full());
    }

    #[test]
    fn should_not_initialize_indexes_cache_when_disabled() {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let partition_path = "/topics/1/1";
        let start_offset = 0;
        let config = Arc::new(SegmentConfig {
            cache_indexes: false,
            ..SegmentConfig::default()
        });

        let segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            partition_path,
            config,
            storage,
        );

        assert!(segment.indexes.is_none());
    }

    #[test]
    fn should_not_initialize_time_indexes_cache_when_disabled() {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let partition_path = "/topics/2/3";
        let start_offset = 0;
        let config = Arc::new(SegmentConfig {
            cache_time_indexes: false,
            ..SegmentConfig::default()
        });

        let segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            partition_path,
            config,
            storage,
        );

        assert!(segment.time_indexes.is_none());
    }
}
