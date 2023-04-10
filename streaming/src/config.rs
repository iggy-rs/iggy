use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize)]
pub struct StreamConfig {
    pub topic: Arc<TopicConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TopicConfig {
    pub partition: Arc<PartitionConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PartitionConfig {
    pub segment: Arc<SegmentConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SegmentConfig {
    pub size_bytes: u64,
    pub messages_required_to_save: u64,
    pub messages_buffer: u64,
}
