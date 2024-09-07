use crate::configs::resource_quota::MemoryResourceQuota;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use iggy::{
    compression::compression_algorithm::CompressionAlgorithm, utils::duration::IggyDuration,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DisplayFromStr;

#[derive(Debug, Deserialize, Serialize)]
pub struct SystemConfig {
    pub path: String,
    pub backup: BackupConfig,
    pub database: Option<DatabaseConfig>,
    pub state: StateConfig,
    pub runtime: RuntimeConfig,
    pub logging: LoggingConfig,
    pub cache: CacheConfig,
    pub stream: StreamConfig,
    pub topic: TopicConfig,
    pub partition: PartitionConfig,
    pub segment: SegmentConfig,
    pub encryption: EncryptionConfig,
    pub compression: CompressionConfig,
    pub message_deduplication: MessageDeduplicationConfig,
    pub recovery: RecoveryConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BackupConfig {
    pub path: String,
    pub compatibility: CompatibilityConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CompatibilityConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RuntimeConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CompressionConfig {
    pub allow_override: bool,
    pub default_algorithm: CompressionAlgorithm,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct LoggingConfig {
    pub path: String,
    pub level: String,
    pub max_size: IggyByteSize,
    #[serde_as(as = "DisplayFromStr")]
    pub retention: IggyDuration,
    #[serde_as(as = "DisplayFromStr")]
    pub sysinfo_print_interval: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CacheConfig {
    pub enabled: bool,
    pub size: MemoryResourceQuota,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EncryptionConfig {
    pub enabled: bool,
    pub key: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StreamConfig {
    pub path: String,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct TopicConfig {
    pub path: String,
    #[serde_as(as = "DisplayFromStr")]
    pub max_size: MaxTopicSize,
    pub delete_oldest_segments: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PartitionConfig {
    pub path: String,
    pub messages_required_to_save: u32,
    pub enforce_fsync: bool,
    pub validate_checksum: bool,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct MessageDeduplicationConfig {
    pub enabled: bool,
    pub max_entries: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub expiry: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RecoveryConfig {
    pub recreate_missing_state: bool,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct SegmentConfig {
    pub size: IggyByteSize,
    pub cache_indexes: bool,
    pub cache_time_indexes: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub message_expiry: IggyExpiry,
    pub archive_expired: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StateConfig {
    pub enforce_fsync: bool,
}

impl SystemConfig {
    pub fn get_system_path(&self) -> String {
        self.path.to_string()
    }

    pub fn get_database_path(&self) -> Option<String> {
        self.database
            .as_ref()
            .map(|database| format!("{}/{}", self.get_system_path(), database.path))
    }

    pub fn get_state_path(&self) -> String {
        format!("{}/state", self.get_system_path())
    }

    pub fn get_state_log_path(&self) -> String {
        format!("{}/log", self.get_state_path())
    }

    pub fn get_state_info_path(&self) -> String {
        format!("{}/info", self.get_state_path())
    }
    pub fn get_state_tokens_path(&self) -> String {
        format!("{}/tokens", self.get_state_path())
    }

    pub fn get_backup_path(&self) -> String {
        format!("{}/{}", self.get_system_path(), self.backup.path)
    }

    pub fn get_compatibility_backup_path(&self) -> String {
        format!(
            "{}/{}",
            self.get_backup_path(),
            self.backup.compatibility.path
        )
    }

    pub fn get_runtime_path(&self) -> String {
        format!("{}/{}", self.get_system_path(), self.runtime.path)
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

    pub fn get_offsets_path(&self, stream_id: u32, topic_id: u32, partition_id: u32) -> String {
        format!(
            "{}/offsets",
            self.get_partition_path(stream_id, topic_id, partition_id)
        )
    }

    pub fn get_consumer_offsets_path(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
    ) -> String {
        format!(
            "{}/consumers",
            self.get_offsets_path(stream_id, topic_id, partition_id)
        )
    }

    pub fn get_consumer_group_offsets_path(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
    ) -> String {
        format!(
            "{}/groups",
            self.get_offsets_path(stream_id, topic_id, partition_id)
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
