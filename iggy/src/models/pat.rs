use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RawPersonalAccessToken {
    pub token: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PersonalAccessTokenInfo {
    pub name: String,
    pub expiry: Option<u64>,
}
