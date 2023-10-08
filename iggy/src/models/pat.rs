use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RawPersonalAccessToken {
    pub token: String,
}
