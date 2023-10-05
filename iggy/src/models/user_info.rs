use crate::models::permissions::Permissions;
use crate::models::user_status::UserStatus;
use serde::{Deserialize, Serialize};

pub type UserId = u32;

#[derive(Debug, Serialize, Deserialize)]
pub struct UserInfo {
    pub id: UserId,
    pub created_at: u64,
    pub status: UserStatus,
    pub username: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserInfoDetails {
    pub id: UserId,
    pub created_at: u64,
    pub status: UserStatus,
    pub username: String,
    pub permissions: Option<Permissions>,
}
