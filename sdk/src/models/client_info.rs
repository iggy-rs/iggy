use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientInfo {
    pub id: u32,
    pub address: String,
    pub transport: String,
}
