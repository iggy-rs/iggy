use crate::configs::system::SystemConfig;
use crate::streaming::segments::index::Index;
use crate::streaming::segments::time_index::TimeIndex;
use iggy::error::IggyError;
use iggy::models::messages::Message;
use iggy::utils::timestamp::IggyTimestamp;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use super::paths::{get_index_path, get_log_path, get_time_index_path};
use super::persistence::SegmentPersister;
use super::storage::SegmentStorageVariant;
pub const MAX_SIZE_BYTES: u32 = 1000 * 1000 * 1000;

#[derive(Debug)]
pub struct Segment {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub current_offset: u64,
    pub path: String,
    pub log_path: String,
    pub index_path: String,
    pub time_index_path: String,
    pub size_bytes: u32,
    pub size_of_parent_stream: Arc<AtomicU64>,
    pub size_of_parent_topic: Arc<AtomicU64>,
    pub size_of_parent_partition: Arc<AtomicU64>,
    pub messages_count_of_parent_stream: Arc<AtomicU64>,
    pub messages_count_of_parent_topic: Arc<AtomicU64>,
    pub messages_count_of_parent_partition: Arc<AtomicU64>,
    pub is_closed: bool,
    pub(crate) message_expiry: Option<u32>,
    pub(crate) unsaved_messages: Option<Vec<Arc<Message>>>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) indexes: Option<Vec<Index>>,
    pub(crate) time_indexes: Option<Vec<TimeIndex>>,
    pub(crate) storage: SegmentStorageVariant,
}

impl Segment {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        start_offset: u64,
        config: Arc<SystemConfig>,
        message_expiry: Option<u32>,
        size_of_parent_stream: Arc<AtomicU64>,
        size_of_parent_topic: Arc<AtomicU64>,
        size_of_parent_partition: Arc<AtomicU64>,
        messages_count_of_parent_stream: Arc<AtomicU64>,
        messages_count_of_parent_topic: Arc<AtomicU64>,
        messages_count_of_parent_partition: Arc<AtomicU64>,
    ) -> Segment {
        let path = config.get_segment_path(stream_id, topic_id, partition_id, start_offset);
        let enforce_fsync = config.partition.enforce_fsync;
        Segment {
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            end_offset: 0,
            current_offset: start_offset,
            path: path.clone(),
            log_path: get_log_path(path.clone().as_str()),
            index_path: get_index_path(path.clone().as_str()),
            time_index_path: get_time_index_path(path.clone().as_str()),
            size_bytes: 0,
            message_expiry,
            indexes: match config.segment.cache_indexes {
                true => Some(Vec::new()),
                false => None,
            },
            time_indexes: match config.segment.cache_time_indexes {
                true => Some(Vec::new()),
                false => None,
            },
            unsaved_messages: None,
            is_closed: false,
            size_of_parent_stream,
            size_of_parent_partition,
            size_of_parent_topic,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
            config,
            storage: SegmentStorageVariant::new(enforce_fsync, path),
        }
    }

    pub async fn append(&self, messages: Arc<Vec<Message>>) -> Result<(), IggyError> {
        self.storage.append(messages, self.current_offset).await
    }

    pub async fn is_full(&self) -> bool {
        if self.size_bytes >= self.config.segment.size.as_bytes_u64() as u32 {
            return true;
        }

        self.is_expired(IggyTimestamp::now().to_micros()).await
    }

    pub async fn is_expired(&self, now: u64) -> bool {
        if self.message_expiry.is_none() {
            return false;
        }

        let last_messages = self.get_messages(self.end_offset, 1).await;
        if last_messages.is_err() {
            return false;
        }

        let last_messages = last_messages.unwrap();
        if last_messages.is_empty() {
            return false;
        }

        let last_message = last_messages[0].as_ref();
        let message_expiry = (self.message_expiry.unwrap() * 1000) as u64;
        (last_message.timestamp + message_expiry) <= now
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::SegmentConfig;

    #[tokio::test]
    async fn should_be_created_given_valid_parameters() {
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let start_offset = 0;
        let config = Arc::new(SystemConfig::default());
        let path = config.get_segment_path(stream_id, topic_id, partition_id, start_offset);
        let message_expiry = Some(10);
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let size_of_parent_topic = Arc::new(AtomicU64::new(0));
        let size_of_parent_partition = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_topic = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_partition = Arc::new(AtomicU64::new(0));

        let segment = Segment::new(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            config,
            message_expiry,
            size_of_parent_stream,
            size_of_parent_topic,
            size_of_parent_partition,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
        );

        assert_eq!(segment.stream_id, stream_id);
        assert_eq!(segment.topic_id, topic_id);
        assert_eq!(segment.partition_id, partition_id);
        assert_eq!(segment.start_offset, start_offset);
        assert_eq!(segment.current_offset, 0);
        assert_eq!(segment.end_offset, 0);
        assert_eq!(segment.size_bytes, 0);
        assert_eq!(segment.path, path);
        assert_eq!(segment.message_expiry, message_expiry);
        assert!(segment.unsaved_messages.is_none());
        assert!(segment.indexes.is_some());
        assert!(segment.time_indexes.is_some());
        assert!(!segment.is_closed);
        assert!(!segment.is_full().await);
    }

    #[test]
    fn should_not_initialize_indexes_cache_when_disabled() {
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let start_offset = 0;
        let config = Arc::new(SystemConfig {
            segment: SegmentConfig {
                cache_indexes: false,
                ..Default::default()
            },
            ..Default::default()
        });
        let message_expiry = None;
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let size_of_parent_topic = Arc::new(AtomicU64::new(0));
        let size_of_parent_partition = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_topic = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_partition = Arc::new(AtomicU64::new(0));

        let segment = Segment::new(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            config,
            message_expiry,
            size_of_parent_stream,
            size_of_parent_topic,
            size_of_parent_partition,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
        );

        assert!(segment.indexes.is_none());
    }

    #[test]
    fn should_not_initialize_time_indexes_cache_when_disabled() {
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let start_offset = 0;
        let config = Arc::new(SystemConfig {
            segment: SegmentConfig {
                cache_time_indexes: false,
                ..Default::default()
            },
            ..Default::default()
        });
        let message_expiry = None;
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let size_of_parent_topic = Arc::new(AtomicU64::new(0));
        let size_of_parent_partition = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_topic = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_partition = Arc::new(AtomicU64::new(0));

        let segment = Segment::new(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            config,
            message_expiry,
            size_of_parent_stream,
            size_of_parent_topic,
            size_of_parent_partition,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
        );
        assert!(segment.time_indexes.is_none());
    }
}
