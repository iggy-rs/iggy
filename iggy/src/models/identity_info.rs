use serde::{Deserialize, Serialize};

use super::user_info::UserId;

#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityInfo {
    pub user_id: UserId,
    pub token: Option<TokenInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    pub access_token: String,
    pub refresh_token: String,
    pub expiry: u64,
}
