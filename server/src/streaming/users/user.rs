use crate::streaming::utils::crypto;
use iggy::models::permissions::Permissions;
use iggy::models::user_status::UserStatus;
use iggy::utils::timestamp::TimeStamp;
use iggy::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_USER_ID};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    pub id: u32,
    pub status: UserStatus,
    pub username: String,
    pub password: String,
    pub created_at: u64,
    pub permissions: Option<Permissions>,
}

impl Default for User {
    fn default() -> Self {
        Self {
            id: 1,
            status: UserStatus::Active,
            username: "user".to_string(),
            password: "secret".to_string(),
            created_at: TimeStamp::now().to_micros(),
            permissions: None,
        }
    }
}

impl User {
    pub fn empty(id: u32) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    pub fn new(id: u32, username: &str, password: &str, permissions: Option<Permissions>) -> Self {
        Self {
            id,
            username: username.to_string(),
            password: crypto::hash_password(password),
            created_at: TimeStamp::now().to_micros(),
            status: UserStatus::Active,
            permissions,
        }
    }

    pub fn root() -> Self {
        Self::new(
            DEFAULT_ROOT_USER_ID,
            DEFAULT_ROOT_USERNAME,
            DEFAULT_ROOT_PASSWORD,
            Some(Permissions::root()),
        )
    }

    pub fn is_root(&self) -> bool {
        self.id == DEFAULT_ROOT_USER_ID
    }

    pub fn is_active(&self) -> bool {
        self.status == UserStatus::Active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_root_user_data_and_credentials_should_be_valid() {
        let user = User::root();
        assert_eq!(user.id, DEFAULT_ROOT_USER_ID);
        assert_eq!(user.username, DEFAULT_ROOT_USERNAME);
        assert_ne!(user.password, DEFAULT_ROOT_PASSWORD);
        assert!(crypto::verify_password(
            DEFAULT_ROOT_PASSWORD,
            &user.password
        ));
        assert_eq!(user.status, UserStatus::Active);
        assert!(user.created_at > 0);
    }
}
