use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientInfo {
    pub id: u32,
    pub address: String,
    pub transport: String,
    pub consumer_groups: Vec<ConsumerGroupInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroupInfo {
    pub stream_id: u32,
    pub topic_id: u32,
    pub group_id: u32,
}
