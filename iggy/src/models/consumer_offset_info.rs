use serde::{Deserialize, Serialize};

/// `ConsumerOffsetInfo` represents the information about a consumer offset.
/// It consists of the following fields:
/// - `partition_id`: the unique identifier of the partition.
/// - `current_offset`: the current offset of the partition.
/// - `stored_offset`: the stored offset by the consumer in the partition.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerOffsetInfo {
    /// The unique identifier of the partition.
    pub partition_id: u32,
    /// The current offset of the partition.
    pub current_offset: u64,
    /// The stored offset by the consumer in the partition.
    pub stored_offset: u64,
}
