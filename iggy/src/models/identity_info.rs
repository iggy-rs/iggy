use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityInfo {
    pub user_id: u32,
    pub token: Option<String>,
}
