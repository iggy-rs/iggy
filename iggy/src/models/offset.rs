use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Offset {
    pub consumer_id: u32,
    pub offset: u64,
}
