use crate::utils::byte_size::IggyByteSize;
use crate::{models::topic::Topic, utils::timestamp::IggyTimestamp};
use serde::{Deserialize, Serialize};

/// `Stream` represents the highest level of logical separation of data.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the stream.
/// - `created_at`: the timestamp when the stream was created.
/// - `name`: the unique name of the stream.
/// - `size_bytes`: the total size of the stream in bytes.
/// - `messages_count`: the total number of messages in the stream.
/// - `topics_count`: the total number of topics in the stream.
#[derive(Debug, Serialize, Deserialize)]
pub struct Stream {
    /// The unique identifier (numeric) of the stream.
    pub id: u32,
    /// The timestamp when the stream was created.
    pub created_at: IggyTimestamp,
    /// The unique name of the stream.
    pub name: String,
    /// The total size of the stream in bytes.
    pub size: IggyByteSize,
    /// The total number of messages in the stream.
    pub messages_count: u64,
    /// The total number of topics in the stream.
    pub topics_count: u32,
}

/// `StreamDetails` represents the detailed information about the stream.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the stream.
/// - `created_at`: the timestamp when the stream was created.
/// - `name`: the unique name of the stream.
/// - `size_bytes`: the total size of the stream in bytes.
/// - `messages_count`: the total number of messages in the stream.
/// - `topics_count`: the total number of topics in the stream.
/// - `topics`: the list of topics in the stream.
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamDetails {
    /// The unique identifier (numeric) of the stream.
    pub id: u32,
    /// The timestamp when the stream was created.
    pub created_at: IggyTimestamp,
    /// The unique name of the stream.
    pub name: String,
    /// The total size of the stream in bytes.
    pub size: IggyByteSize,
    /// The total number of messages in the stream.
    pub messages_count: u64,
    /// The total number of topics in the stream.
    pub topics_count: u32,
    /// The collection of topics in the stream.
    pub topics: Vec<Topic>,
}
