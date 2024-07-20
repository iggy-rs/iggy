use crate::utils::byte_size::IggyByteSize;
use crate::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};

/// `Partition` represents the information about a partition.
/// It consists of the following fields:
/// - `id`: unique identifier of the partition.
/// - `created_at`: the timestamp of the partition creation.
/// - `segments_count`: the number of segments in the partition.
/// - `current_offset`: the current offset of the partition.
/// - `size_bytes`: the size of the partition in bytes.
/// - `messages_count`: the number of messages in the partition.
#[derive(Debug, Serialize, Deserialize)]
pub struct Partition {
    /// Unique identifier of the partition.
    pub id: u32,
    /// The timestamp of the partition creation.
    pub created_at: IggyTimestamp,
    /// The number of segments in the partition.
    pub segments_count: u32,
    /// The current offset of the partition.
    pub current_offset: u64,
    /// The size of the partition in bytes.
    pub size: IggyByteSize,
    /// The number of messages in the partition.
    pub messages_count: u64,
}
