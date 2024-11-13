use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot(pub Vec<u8>);

impl Snapshot {
    pub fn new(data: Vec<u8>) -> Self {
        Snapshot(data)
    }
}
