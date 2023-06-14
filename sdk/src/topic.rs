use crate::partition::Partition;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Topic {
    pub id: u32,
    pub name: String,
    pub partitions_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicDetails {
    pub id: u32,
    pub name: String,
    pub partitions_count: u32,
    pub partitions: Vec<Partition>,
}
