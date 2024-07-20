use crate::models::permissions::Permissions;
use crate::models::user_status::UserStatus;
use crate::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU32;

/// `UserId` represents the unique identifier (numeric) of the user.
pub type UserId = u32;
/// `AtomicUserId` represents the unique identifier (numeric) of the user
/// which can be safely modified concurrently across threads
pub type AtomicUserId = AtomicU32;

/// `UserInfo` represents the basic information about the user.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the user.
/// - `created_at`: the timestamp when the user was created.
/// - `status`: the status of the user.
/// - `username`: the username of the user.
#[derive(Debug, Serialize, Deserialize)]
pub struct UserInfo {
    /// The unique identifier (numeric) of the user.
    pub id: UserId,
    /// The timestamp when the user was created.
    pub created_at: IggyTimestamp,
    /// The status of the user.
    pub status: UserStatus,
    /// The username of the user.
    pub username: String,
}

/// `UserInfoDetails` represents the detailed information about the user.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the user.
/// - `created_at`: the timestamp when the user was created.
/// - `status`: the status of the user.
/// - `username`: the username of the user.
/// - `permissions`: the optional permissions of the user.
#[derive(Debug, Serialize, Deserialize)]
pub struct UserInfoDetails {
    /// The unique identifier (numeric) of the user.
    pub id: UserId,
    /// The timestamp when the user was created.
    pub created_at: IggyTimestamp,
    /// The status of the user.
    pub status: UserStatus,
    /// The username of the user.
    pub username: String,
    /// The optional permissions of the user.
    pub permissions: Option<Permissions>,
}
