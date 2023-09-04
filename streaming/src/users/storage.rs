use crate::storage::{Storage, UserStorage};
use crate::users::user::User;
use async_trait::async_trait;
use iggy::error::Error;
use sled::Db;
use std::sync::Arc;
use tracing::info;

const KEY_PREFIX: &str = "users:";

#[derive(Debug)]
pub struct FileUserStorage {
    db: Arc<Db>,
}

impl FileUserStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

unsafe impl Send for FileUserStorage {}
unsafe impl Sync for FileUserStorage {}

#[async_trait]
impl UserStorage for FileUserStorage {
    async fn load_all(&self) -> Result<Vec<User>, Error> {
        let users = self
            .db
            .scan_prefix(KEY_PREFIX)
            .map(|x| {
                let data = x.unwrap().1;
                let user = rmp_serde::from_slice::<User>(&data);
                user.unwrap()
            })
            .collect();

        Ok(users)
    }
}

#[async_trait]
impl Storage<User> for FileUserStorage {
    async fn load(&self, user: &mut User) -> Result<(), Error> {
        let user_data = self.db.get(get_key(user.id));
        if user_data.is_err() {
            // TODO: Return error
            panic!("Cannot load user with ID: {}", user.id);
        }

        let user_data = user_data.unwrap();
        if user_data.is_none() {
            // TODO: Return error
            panic!("Cannot load user with ID: {}", user.id);
        }

        let user_data = user_data.unwrap();
        let user_data = rmp_serde::from_slice::<User>(&user_data);
        if user_data.is_err() {
            // TODO: Return error
            panic!("Cannot load user with ID: {}", user.id);
        }

        let user_data = user_data.unwrap();
        user.role = user_data.role;
        user.status = user_data.status;
        user.username = user_data.username;
        user.password = user_data.password;
        user.created_at = user_data.created_at;
        user.permissions = user_data.permissions;
        Ok(())
    }

    async fn save(&self, user: &User) -> Result<(), Error> {
        if self
            .db
            .insert(get_key(user.id), rmp_serde::to_vec(&user).unwrap())
            .is_err()
        {
            // TODO: Return error
            panic!("Cannot save user with ID: {}", user.id);
        }

        info!("Saved user with ID: {}.", user.id);
        Ok(())
    }

    async fn delete(&self, user: &User) -> Result<(), Error> {
        info!("Deleting user with ID: {}...", user.id);
        if self.db.remove(get_key(user.id)).is_err() {
            // TODO: Return error
            panic!("Cannot delete user with ID: {}", user.id);
        }
        info!("Deleted user with ID: {}.", user.id);
        Ok(())
    }
}

fn get_key(user_id: u32) -> String {
    format!("{}:{}", KEY_PREFIX, user_id)
}
