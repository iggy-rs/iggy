use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Stream {
    pub id: u32,
    pub topics: u32,
    pub name: String,
}
