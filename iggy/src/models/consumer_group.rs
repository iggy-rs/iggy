use serde::{Deserialize, Serialize};

/// `ConsumerGroup` represents the information about a consumer group.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the consumer group.
/// - `name`: the name of the consumer group.
/// - `partitions_count`: the number of partitions the consumer group is consuming.
/// - `members_count`: the number of members in the consumer group.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroup {
    /// The unique identifier (numeric) of the consumer group.
    pub id: u32,
    /// The name of the consumer group.
    pub name: String,
    /// The number of partitions the consumer group is consuming.
    pub partitions_count: u32,
    /// The number of members in the consumer group.
    pub members_count: u32,
}

/// `ConsumerGroupDetails` represents the detailed information about a consumer group.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the consumer group.
/// - `name`: the name of the consumer group.
/// - `partitions_count`: the number of partitions the consumer group is consuming.
/// - `members_count`: the number of members in the consumer group.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroupDetails {
    /// The unique identifier (numeric) of the consumer group.
    pub id: u32,
    /// The name of the consumer group.
    pub name: String,
    /// The number of partitions the consumer group is consuming.
    pub partitions_count: u32,
    /// The number of members in the consumer group.
    pub members_count: u32,
    /// The collection of members in the consumer group.
    pub members: Vec<ConsumerGroupMember>,
}

/// `ConsumerGroupMember` represents the information about a consumer group member.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the consumer group member.
/// - `partitions_count`: the number of partitions the consumer group member is consuming.
/// - `partitions`: the collection of partitions the consumer group member is consuming.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroupMember {
    /// The unique identifier (numeric) of the consumer group member.
    pub id: u32,
    /// The number of partitions the consumer group member is consuming.
    pub partitions_count: u32,
    /// The collection of partitions the consumer group member is consuming.
    pub partitions: Vec<u32>,
}
