use serde::{Deserialize, Serialize};

// TODO: Refactor UserClient trait to use this struct for login
#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityToken {
    pub user_id: u32,
    pub token: Option<String>,
}
