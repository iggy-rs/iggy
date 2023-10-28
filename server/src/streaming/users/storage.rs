use crate::streaming::storage::{Storage, UserStorage};
use crate::streaming::users::user::User;
use anyhow::Context;
use async_trait::async_trait;
use iggy::error::Error;
use iggy::models::user_info::UserId;
use sled::Db;
use std::sync::Arc;
use tracing::info;

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
        let user_id = self.db.get(&user_id_key).with_context(|| {
            format!(
                "Failed to load user with key: {}, username: {}",
                user_id_key, username
            )
        });
        match user_id {
            Ok(user_id) => {
                if let Some(user_id) = user_id {
                    let user_id = u32::from_le_bytes(user_id.as_ref().try_into()?);
                    let mut user = User::empty(user_id);
                    self.load(&mut user).await?;
                    Ok(user)
                } else {
                    Err(Error::ResourceNotFound(user_id_key))
                }
            }
            Err(err) => Err(Error::CannotLoadResource(err)),
        }
    }

    async fn load_all(&self) -> Result<Vec<User>, Error> {
        let mut users = Vec::new();
        for data in self.db.scan_prefix(format!("{}:", KEY_PREFIX)) {
            let user = match data.with_context(|| {
                format!(
                    "Failed to load user, when searching for key: {}",
                    KEY_PREFIX
                )
            }) {
                Ok((_, value)) => match rmp_serde::from_slice::<User>(&value).with_context(|| {
                    format!(
                        "Failed to deserialize user, when searching for key: {}",
                        KEY_PREFIX
                    )
                }) {
                    Ok(user) => user,
                    Err(err) => {
                        return Err(Error::CannotDeserializeResource(err));
                    }
                },
                Err(err) => {
                    return Err(Error::CannotLoadResource(err));
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
        let user_data = match self.db.get(&key).with_context(|| {
            format!(
                "Failed to load user with key: {}, username: {}",
                key, user.username
            )
        }) {
            Ok(data) => {
                if let Some(user_data) = data {
                    user_data
                } else {
                    return Err(Error::ResourceNotFound(key));
                }
            }
            Err(err) => {
                return Err(Error::CannotLoadResource(err));
            }
        };

        let user_data = rmp_serde::from_slice::<User>(&user_data)
            .with_context(|| format!("Failed to deserialize user with key: {}", key));
        match user_data {
            Ok(user_data) => {
                user.status = user_data.status;
                user.username = user_data.username;
                user.password = user_data.password;
                user.created_at = user_data.created_at;
                user.permissions = user_data.permissions;
                Ok(())
            }
            Err(err) => {
                return Err(Error::CannotDeserializeResource(err));
            }
        }
    }

    async fn save(&self, user: &User) -> Result<(), Error> {
        let key = get_key(user.id);
        match rmp_serde::to_vec(&user)
            .with_context(|| format!("Failed to serialize user with key: {}", key))
        {
            Ok(data) => {
                if let Err(err) = self
                    .db
                    .insert(&key, data)
                    .with_context(|| format!("Failed to insert user with key: {}", key))
                {
                    return Err(Error::CannotSaveResource(err));
                }
                if let Err(err) = self
                    .db
                    .insert(get_id_key(&user.username), &user.id.to_le_bytes())
                    .with_context(|| {
                        format!(
                            "Failed to insert user with ID: {} key: {}",
                            &user.id,
                            get_id_key(&user.username)
                        )
                    })
                {
                    return Err(Error::CannotSaveResource(err));
                }
            }
            Err(err) => {
                return Err(Error::CannotSerializeResource(err));
            }
        }

        info!("Saved user with ID: {}.", user.id);
        Ok(())
    }

    async fn delete(&self, user: &User) -> Result<(), Error> {
        info!("Deleting user with ID: {}...", user.id);
        let key = get_key(user.id);
        if let Err(err) = self
            .db
            .remove(&key)
            .with_context(|| format!("Failed to delete user with ID: {}, key: {}", user.id, key))
        {
            return Err(Error::CannotDeleteResource(err));
        } else {
            let key = get_id_key(&user.username);
            if let Err(err) = self.db.remove(&key).with_context(|| {
                format!("Failed to delete user with ID: {}, key : {}", user.id, key)
            }) {
                return Err(Error::CannotDeleteResource(err));
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
