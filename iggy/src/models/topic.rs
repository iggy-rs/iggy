use crate::models::partition::Partition;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Topic {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size_bytes: u64,
    pub message_expiry: Option<u32>,
    pub messages_count: u64,
    pub partitions_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicDetails {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size_bytes: u64,
    pub message_expiry: Option<u32>,
    pub messages_count: u64,
    pub partitions_count: u32,
    pub partitions: Vec<Partition>,
}
