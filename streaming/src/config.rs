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
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SegmentConfig {
    pub size_bytes: u32,
    pub messages_required_to_save: u32,
    pub messages_buffer: u32,
}
