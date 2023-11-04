use crate::models::partition::Partition;
use serde::{Deserialize, Serialize};

/// `Topic` represents the medium level of logical separation of data as it's a part of the stream.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the topic.
/// - `created_at`: the timestamp when the topic was created.
/// - `name`: the unique name of the topic.
/// - `size_bytes`: the total size of the topic in bytes.
/// - `message_expiry`: the optional expiry of the messages in the topic.
/// - `messages_count`: the total number of messages in the topic.
/// - `partitions_count`: the total number of partitions in the topic.
#[derive(Debug, Serialize, Deserialize)]
pub struct Topic {
    /// The unique identifier (numeric) of the topic.
    pub id: u32,
    /// The timestamp when the topic was created.
    pub created_at: u64,
    /// The unique name of the topic.
    pub name: String,
    /// The total size of the topic in bytes.
    pub size_bytes: u64,
    /// The optional expiry of the messages in the topic.
    pub message_expiry: Option<u32>,
    /// The total number of messages in the topic.
    pub messages_count: u64,
    /// The total number of partitions in the topic.
    pub partitions_count: u32,
}

/// `TopicDetails` represents the detailed information about the topic.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the topic.
/// - `created_at`: the timestamp when the topic was created.
/// - `name`: the unique name of the topic.
/// - `size_bytes`: the total size of the topic in bytes.
/// - `message_expiry`: the optional expiry of the messages in the topic.
/// - `messages_count`: the total number of messages in the topic.
/// - `partitions_count`: the total number of partitions in the topic.
/// - `partitions`: the collection of partitions in the topic.
#[derive(Debug, Serialize, Deserialize)]
pub struct TopicDetails {
    /// The unique identifier (numeric) of the topic.
    pub id: u32,
    /// The timestamp when the topic was created.
    pub created_at: u64,
    /// The unique name of the topic.
    pub name: String,
    /// The total size of the topic in bytes.
    pub size_bytes: u64,
    /// The optional expiry of the messages in the topic.
    pub message_expiry: Option<u32>,
    /// The total number of messages in the topic.
    pub messages_count: u64,
    /// The total number of partitions in the topic.
    pub partitions_count: u32,
    /// The collection of partitions in the topic.
    pub partitions: Vec<Partition>,
}
