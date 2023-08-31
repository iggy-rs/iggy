use crate::models::topic::Topic;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Stream {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub topics_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamDetails {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub topics_count: u32,
    pub topics: Vec<Topic>,
}
