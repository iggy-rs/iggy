use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct SystemConfig {
    pub path: String,
    pub database: DatabaseConfig,
    pub stream: StreamConfig,
    pub topic: TopicConfig,
    pub partition: PartitionConfig,
    pub segment: SegmentConfig,
    pub encryption: EncryptionConfig,
    pub user: UserConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct EncryptionConfig {
    pub enabled: bool,
    pub key: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct UserConfig {
    pub authorization_enabled: bool,
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
            database: DatabaseConfig::default(),
            stream: StreamConfig::default(),
            encryption: EncryptionConfig::default(),
            topic: TopicConfig::default(),
            partition: PartitionConfig::default(),
            segment: SegmentConfig::default(),
            user: UserConfig::default(),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> DatabaseConfig {
        DatabaseConfig {
            path: "database".to_string(),
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

impl SystemConfig {
    pub fn get_system_path(&self) -> String {
        self.path.to_string()
    }

    pub fn get_database_path(&self) -> String {
        format!("{}/{}", self.get_system_path(), self.database.path)
    }

    pub fn get_streams_path(&self) -> String {
        format!("{}/{}", self.get_system_path(), self.stream.path)
    }

    pub fn get_stream_path(&self, stream_id: u32) -> String {
        format!("{}/{}", self.get_streams_path(), stream_id)
    }

    pub fn get_topics_path(&self, stream_id: u32) -> String {
        format!("{}/{}", self.get_stream_path(stream_id), self.topic.path)
    }

    pub fn get_topic_path(&self, stream_id: u32, topic_id: u32) -> String {
        format!("{}/{}", self.get_topics_path(stream_id), topic_id)
    }

    pub fn get_partitions_path(&self, stream_id: u32, topic_id: u32) -> String {
        format!(
            "{}/{}",
            self.get_topic_path(stream_id, topic_id),
            self.partition.path
        )
    }

    pub fn get_partition_path(&self, stream_id: u32, topic_id: u32, partition_id: u32) -> String {
        format!(
            "{}/{}",
            self.get_partitions_path(stream_id, topic_id),
            partition_id
        )
    }

    pub fn get_segment_path(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        start_offset: u64,
    ) -> String {
        format!(
            "{}/{:0>20}",
            self.get_partition_path(stream_id, topic_id, partition_id),
            start_offset
        )
    }
}
