use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerOffsetInfo {
    pub partition_id: u32,
    pub current_offset: u64,
    pub stored_offset: u64,
}
