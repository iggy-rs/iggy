use crate::streaming::storage::{Storage, UserStorage};
use crate::streaming::users::user::User;
use async_trait::async_trait;
use iggy::error::Error;
use iggy::models::user_info::UserId;
use sled::Db;
use std::sync::Arc;
use tracing::{error, info};

const KEY_PREFIX: &str = "users";

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
    async fn load_by_id(&self, id: UserId) -> Result<User, Error> {
        let mut user = User::empty(id);
        self.load(&mut user).await?;
        Ok(user)
    }

    async fn load_by_username(&self, username: &str) -> Result<User, Error> {
        let user_id_key = get_id_key(username);
        let user_id = self.db.get(&user_id_key);
        match user_id {
            Ok(user_id) => {
                if let Some(user_id) = user_id {
                    let user_id = u32::from_le_bytes(user_id.as_ref().try_into()?);
                    let mut user = User::empty(user_id);
                    self.load(&mut user).await?;
                    Ok(user)
                } else {
                    error!("Cannot find user with username: {}", username);
                    Err(Error::CannotLoadResource(user_id_key))
                }
            }
            Err(error) => {
                error!(
                    "Cannot load user with username: {}. Error: {}",
                    username, error
                );
                Err(Error::CannotLoadResource(user_id_key))
            }
        }
    }

    async fn load_all(&self) -> Result<Vec<User>, Error> {
        let mut users = Vec::new();
        for data in self.db.scan_prefix(format!("{}:", KEY_PREFIX)) {
            let user = match data {
                Ok((_, value)) => match rmp_serde::from_slice::<User>(&value) {
                    Ok(user) => user,
                    Err(err) => {
                        error!("Cannot deserialize user. Error: {}", err);
                        return Err(Error::CannotDeserializeResource(KEY_PREFIX.to_string()));
                    }
                },
                Err(err) => {
                    error!("Cannot load user. Error: {}", err);
                    return Err(Error::CannotLoadResource(KEY_PREFIX.to_string()));
                }
            };
            users.push(user);
        }

        Ok(users)
    }
}

#[async_trait]
impl Storage<User> for FileUserStorage {
    async fn load(&self, user: &mut User) -> Result<(), Error> {
        let key = get_key(user.id);
        let user_data = match self.db.get(&key) {
            Ok(data) => {
                if let Some(user_data) = data {
                    user_data
                } else {
                    error!("Cannot find user with username: {}", user.username);
                    return Err(Error::CannotLoadResource(key));
                }
            }
            Err(error) => {
                error!(
                    "Cannot load user with username: {}. Error: {}",
                    user.username, error
                );
                return Err(Error::CannotLoadResource(key));
            }
        };

        let user_data = rmp_serde::from_slice::<User>(&user_data);
        match user_data {
            Ok(user_data) => {
                user.status = user_data.status;
                user.username = user_data.username;
                user.password = user_data.password;
                user.created_at = user_data.created_at;
                user.permissions = user_data.permissions;
                Ok(())
            }
            Err(error) => {
                error!(
                    "Cannot deserialize user with username: {}. Error: {}",
                    user.username, error
                );
                return Err(Error::CannotDeserializeResource(key));
            }
        }
    }

    async fn save(&self, user: &User) -> Result<(), Error> {
        let key = get_key(user.id);
        match rmp_serde::to_vec(&user) {
            Ok(data) => {
                if let Err(err) = self.db.insert(&key, data) {
                    error!("Cannot save user with ID: {}. Error: {}", user.id, err);
                    return Err(Error::CannotSaveResource(key.to_string()));
                }
                if let Err(err) = self
                    .db
                    .insert(get_id_key(&user.username), &user.id.to_le_bytes())
                {
                    error!(
                        "Cannot save username for user with ID: {}. Error: {}",
                        user.id, err
                    );
                    return Err(Error::CannotSaveResource(key.to_string()));
                }
            }
            Err(err) => {
                error!("Cannot serialize user with ID: {}. Error: {}", user.id, err);
                return Err(Error::CannotSerializeResource(key));
            }
        }

        info!("Saved user with ID: {}.", user.id);
        Ok(())
    }

    async fn delete(&self, user: &User) -> Result<(), Error> {
        info!("Deleting user with ID: {}...", user.id);
        let key = get_key(user.id);
        if let Err(error) = self.db.remove(&key) {
            error!("Cannot delete user with ID: {}. Error: {}", user.id, error);
            return Err(Error::CannotDeleteResource(key));
        } else {
            let key = get_id_key(&user.username);
            if let Err(error) = self.db.remove(&key) {
                error!(
                    "Cannot delete username for user with ID: {}. Error: {}",
                    user.id, error
                );
                return Err(Error::CannotDeleteResource(key));
            } else {
                info!("Deleted user with ID: {}.", user.id);
                Ok(())
            }
        }
    }
}

fn get_key(user_id: UserId) -> String {
    format!("{}:{}", KEY_PREFIX, user_id)
}

fn get_id_key(username: &str) -> String {
    format!("{}_id:{}", KEY_PREFIX, username)
}
