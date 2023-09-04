use crate::users::permissions::Permissions;
use iggy::utils::timestamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    pub id: u32,
    pub role: Role,
    pub status: Status,
    pub username: String,
    pub password: String,
    pub created_at: u64,
    pub permissions: Option<Permissions>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Role {
    User,
    Admin,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Status {
    Active,
    Inactive,
}

impl Default for User {
    fn default() -> Self {
        Self {
            id: 1,
            role: Role::User,
            status: Status::Active,
            username: "user".to_string(),
            password: "secret".to_string(),
            created_at: timestamp::get(),
            permissions: None,
        }
    }
}
