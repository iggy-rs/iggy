use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroup {
    pub id: u32,
    pub members_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroupDetails {
    pub id: u32,
    pub members_count: u32,
    pub members: Vec<ConsumerGroupMember>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroupMember {
    pub id: u32,
    pub partitions_count: u32,
    pub partitions: Vec<u32>,
}
