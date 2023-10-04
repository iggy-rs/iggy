use serde::{Deserialize, Serialize};

use super::user_info::UserId;

#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityInfo {
    pub user_id: UserId,
    pub token: Option<String>,
}
