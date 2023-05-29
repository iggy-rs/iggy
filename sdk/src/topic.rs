use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Topic {
    pub id: u32,
    pub partitions: u32,
    pub name: String,
}
