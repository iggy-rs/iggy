use serde::{Deserialize, Serialize};

/// `ClientInfo` represents the information about a client.
/// It consists of the following fields:
/// - `client_id`: the unique identifier of the client.
/// - `user_id`: the unique identifier of the user. This field is optional, as the client might be connected but not authenticated yet.
/// - `address`: the remote address of the client.
/// - `transport`: the transport protocol used by the client.
/// - `consumer_groups_count`: the number of consumer groups the client is part of.
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientInfo {
    /// The unique identifier of the client.
    pub client_id: u32,
    /// The unique identifier of the user. This field is optional, as the client might be connected but not authenticated yet.
    pub user_id: Option<u32>,
    /// The remote address of the client.
    pub address: String,
    /// The transport protocol used by the client.
    pub transport: String,
    /// The number of consumer groups the client is part of.
    pub consumer_groups_count: u32,
}

/// `ClientInfoDetails` represents the detailed information about a client.
/// It consists of the following fields:
/// - `client_id`: the unique identifier of the client.
/// - `user_id`: the unique identifier of the user. This field is optional, as the client might be connected but not authenticated yet.
/// - `address`: the remote address of the client.
/// - `transport`: the transport protocol used by the client.
/// - `consumer_groups_count`: the number of consumer groups the client is part of.
/// - `consumer_groups`: the collection of consumer groups the client is part of.
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientInfoDetails {
    /// The unique identifier of the client.
    pub client_id: u32,
    /// The unique identifier of the user. This field is optional, as the client might be connected but not authenticated yet.
    pub user_id: Option<u32>,
    // The remote address of the client.
    pub address: String,
    /// The transport protocol used by the client.
    pub transport: String,
    /// The number of consumer groups the client is part of.
    pub consumer_groups_count: u32,
    /// The collection of consumer groups the client is part of.
    pub consumer_groups: Vec<ConsumerGroupInfo>,
}

/// `ConsumerGroupInfo` represents the information about a consumer group.
/// It consists of the following fields:
/// - `stream_id`: the unique identifier (numeric) of the stream.
/// - `topic_id`: the unique identifier (numeric) of the topic.
/// - `group_id`: the unique identifier (numeric) of the consumer group.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroupInfo {
    /// The unique identifier (numeric) of the stream.
    pub stream_id: u32,
    /// The unique identifier (numeric) of the topic.
    pub topic_id: u32,
    /// The unique identifier (numeric) of the consumer group.
    pub group_id: u32,
}
