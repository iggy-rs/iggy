use crate::users::permissions::Permissions;
use crate::utils::crypto;
use iggy::utils::timestamp;
use serde::{Deserialize, Serialize};

const ROOT_USER_ID: u32 = 1;
const ROOT_USERNAME: &str = "iggy";
const ROOT_PASSWORD: &str = "iggy";

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    pub id: u32,
    pub status: Status,
    pub username: String,
    pub password: String,
    pub created_at: u64,
    pub permissions: Option<Permissions>,
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
            status: Status::Active,
            username: "user".to_string(),
            password: "secret".to_string(),
            created_at: timestamp::get(),
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
            created_at: timestamp::get(),
            status: Status::Active,
            permissions,
        }
    }

    pub fn root() -> Self {
        Self::new(
            ROOT_USER_ID,
            ROOT_USERNAME,
            ROOT_PASSWORD,
            Some(Permissions::root()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_root_user_data_and_credentials_should_be_valid() {
        let user = User::root();
        assert_eq!(user.id, ROOT_USER_ID);
        assert_eq!(user.username, ROOT_USERNAME);
        assert_ne!(user.password, ROOT_PASSWORD);
        assert!(crypto::verify_password(ROOT_PASSWORD, &user.password));
        assert_eq!(user.status, Status::Active);
        assert!(user.created_at > 0);
    }
}
