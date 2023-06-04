use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize)]
pub struct SystemConfig {
    pub path: String,
    pub stream: Arc<StreamConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StreamConfig {
    pub path: String,
    pub topic: Arc<TopicConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TopicConfig {
    pub path: String,
    pub partition: Arc<PartitionConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PartitionConfig {
    pub segment: Arc<SegmentConfig>,
    pub messages_required_to_save: u32,
    pub messages_buffer: u32,
    pub deduplicate_messages: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SegmentConfig {
    pub size_bytes: u32,
    pub cache_indexes: bool,
    pub cache_time_indexes: bool,
}

impl Default for SystemConfig {
    fn default() -> SystemConfig {
        SystemConfig {
            path: "local_data".to_string(),
            stream: Arc::new(StreamConfig::default()),
        }
    }
}

impl Default for StreamConfig {
    fn default() -> StreamConfig {
        StreamConfig {
            path: "streams".to_string(),
            topic: Arc::new(TopicConfig::default()),
        }
    }
}

impl Default for TopicConfig {
    fn default() -> TopicConfig {
        TopicConfig {
            path: "topics".to_string(),
            partition: Arc::new(PartitionConfig::default()),
        }
    }
}

impl Default for PartitionConfig {
    fn default() -> PartitionConfig {
        PartitionConfig {
            segment: Arc::new(SegmentConfig::default()),
            messages_required_to_save: 1000,
            messages_buffer: 1024,
            deduplicate_messages: false,
        }
    }
}

impl Default for SegmentConfig {
    fn default() -> SegmentConfig {
        SegmentConfig {
            size_bytes: 1024 * 1024 * 1024,
            cache_indexes: true,
            cache_time_indexes: true,
        }
    }
}
