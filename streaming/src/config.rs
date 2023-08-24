use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct SystemConfig {
    pub path: String,
    pub stream: StreamConfig,
    pub topic: TopicConfig,
    pub partition: PartitionConfig,
    pub segment: SegmentConfig,
    pub encryption: EncryptionConfig,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct EncryptionConfig {
    pub enabled: bool,
    pub key: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StreamConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TopicConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PartitionConfig {
    pub path: String,
    pub messages_required_to_save: u32,
    pub messages_buffer: u32,
    pub deduplicate_messages: bool,
    pub enforce_fsync: bool,
    pub validate_checksum: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SegmentConfig {
    pub message_expiry: u32,
    pub size_bytes: u32,
    pub cache_indexes: bool,
    pub cache_time_indexes: bool,
}

impl Default for SystemConfig {
    fn default() -> SystemConfig {
        SystemConfig {
            path: "local_data".to_string(),
            stream: StreamConfig::default(),
            encryption: EncryptionConfig::default(),
            topic: TopicConfig::default(),
            partition: PartitionConfig::default(),
            segment: SegmentConfig::default(),
        }
    }
}

impl Default for StreamConfig {
    fn default() -> StreamConfig {
        StreamConfig {
            path: "streams".to_string(),
        }
    }
}

impl Default for TopicConfig {
    fn default() -> TopicConfig {
        TopicConfig {
            path: "topics".to_string(),
        }
    }
}

impl Default for PartitionConfig {
    fn default() -> PartitionConfig {
        PartitionConfig {
            path: "partitions".to_string(),
            messages_required_to_save: 1000,
            messages_buffer: 1024,
            deduplicate_messages: false,
            enforce_fsync: false,
            validate_checksum: false,
        }
    }
}

impl Default for SegmentConfig {
    fn default() -> SegmentConfig {
        SegmentConfig {
            message_expiry: 0,
            size_bytes: 1024 * 1024 * 1024,
            cache_indexes: true,
            cache_time_indexes: true,
        }
    }
}
