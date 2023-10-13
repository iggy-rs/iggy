use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::storage::{PersonalAccessTokenStorage, Storage};
use async_trait::async_trait;
use iggy::error::Error;
use iggy::models::user_info::UserId;
use sled::Db;
use std::str::from_utf8;
use std::sync::Arc;
use tracing::{error, info};

const KEY_PREFIX: &str = "personal_access_token";

#[derive(Debug)]
pub struct FilePersonalAccessTokenStorage {
    db: Arc<Db>,
}

impl FilePersonalAccessTokenStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

unsafe impl Send for FilePersonalAccessTokenStorage {}
unsafe impl Sync for FilePersonalAccessTokenStorage {}

#[async_trait]
impl PersonalAccessTokenStorage for FilePersonalAccessTokenStorage {
    async fn load_all(&self) -> Result<Vec<PersonalAccessToken>, Error> {
        let mut personal_access_tokens = Vec::new();
        for data in self.db.scan_prefix(format!("{}:token:", KEY_PREFIX)) {
            let personal_access_token = match data {
                Ok((_, value)) => match rmp_serde::from_slice::<PersonalAccessToken>(&value) {
                    Ok(personal_access_token) => personal_access_token,
                    Err(err) => {
                        error!("Cannot deserialize personal access token. Error: {}", err);
                        return Err(Error::CannotDeserializeResource(KEY_PREFIX.to_string()));
                    }
                },
                Err(err) => {
                    error!("Cannot load personal access token. Error: {}", err);
                    return Err(Error::CannotLoadResource(KEY_PREFIX.to_string()));
                }
            };
            personal_access_tokens.push(personal_access_token);
        }

        Ok(personal_access_tokens)
    }

    async fn load_for_user(&self, user_id: UserId) -> Result<Vec<PersonalAccessToken>, Error> {
        let mut personal_access_tokens = Vec::new();
        let key = format!("{}:user:{}:", KEY_PREFIX, user_id);
        for data in self.db.scan_prefix(&key) {
            match data {
                Ok((_, value)) => {
                    let token = from_utf8(&value)?;
                    let personal_access_token = self.load_by_token(token).await?;
                    personal_access_tokens.push(personal_access_token);
                }
                Err(err) => {
                    error!("Cannot load personal access token. Error: {}", err);
                    return Err(Error::CannotLoadResource(key));
                }
            };
        }

        Ok(personal_access_tokens)
    }

    async fn load_by_token(&self, token: &str) -> Result<PersonalAccessToken, Error> {
        let key = get_key(token);
        let personal_access_token = self.db.get(&key);
        if personal_access_token.is_err() {
            return Err(Error::CannotLoadResource(key));
        }

        let personal_access_token = personal_access_token.unwrap();
        if personal_access_token.is_none() {
            return Err(Error::CannotLoadResource(key));
        }

        let personal_access_token = personal_access_token.unwrap();
        let personal_access_token =
            rmp_serde::from_slice::<PersonalAccessToken>(&personal_access_token);
        if personal_access_token.is_err() {
            return Err(Error::CannotDeserializeResource(key));
        }

        let personal_access_token = personal_access_token.unwrap();
        Ok(personal_access_token)
    }

    async fn load_by_name(
        &self,
        user_id: UserId,
        name: &str,
    ) -> Result<PersonalAccessToken, Error> {
        let key = get_name_key(user_id, name);
        let token = self.db.get(&key);
        if token.is_err() {
            return Err(Error::CannotLoadResource(key));
        }

        let token = token.unwrap();
        if token.is_none() {
            return Err(Error::CannotLoadResource(key));
        }

        let token = token.unwrap();
        let token = from_utf8(&token);
        if token.is_err() {
            return Err(Error::CannotDeserializeResource(key));
        }

        let token = token.unwrap();
        self.load_by_token(token).await
    }

    async fn delete_for_user(&self, user_id: UserId, name: &str) -> Result<(), Error> {
        let personal_access_token = self.load_by_name(user_id, name).await?;
        info!("Deleting personal access token with name: {name} for user with ID: {user_id}...");
        let key = get_name_key(user_id, name);
        if self.db.remove(&key).is_err() {
            return Err(Error::CannotDeleteResource(key));
        }
        let key = get_key(&personal_access_token.token);
        if self.db.remove(&key).is_err() {
            return Err(Error::CannotDeleteResource(key));
        }
        info!("Deleted personal access token with name: {name} for user with ID: {user_id}.");
        Ok(())
    }
}

#[async_trait]
impl Storage<PersonalAccessToken> for FilePersonalAccessTokenStorage {
    async fn load(&self, personal_access_token: &mut PersonalAccessToken) -> Result<(), Error> {
        self.load_by_name(personal_access_token.user_id, &personal_access_token.name)
            .await?;
        Ok(())
    }

    async fn save(&self, personal_access_token: &PersonalAccessToken) -> Result<(), Error> {
        let key = get_key(&personal_access_token.token);
        match rmp_serde::to_vec(&personal_access_token) {
            Ok(data) => {
                if let Err(err) = self.db.insert(&key, data) {
                    error!(
                        "Cannot save Personal access token for user with ID: {}. Error: {}",
                        personal_access_token.user_id, err
                    );
                    return Err(Error::CannotSaveResource(key.to_string()));
                }
                if let Err(err) = self.db.insert(
                    get_name_key(personal_access_token.user_id, &personal_access_token.name),
                    personal_access_token.token.as_bytes(),
                ) {
                    error!(
                        "Cannot save personal access token for user with ID: {}. Error: {}",
                        personal_access_token.user_id, err
                    );
                    return Err(Error::CannotSaveResource(key.to_string()));
                }
            }
            Err(err) => {
                error!(
                    "Cannot serialize personal access token for user with ID: {}. Error: {}",
                    personal_access_token.user_id, err
                );
                return Err(Error::CannotSerializeResource(key));
            }
        }

        info!(
            "Saved personal access token for user with ID: {}.",
            personal_access_token.user_id
        );
        Ok(())
    }

    async fn delete(&self, personal_access_token: &PersonalAccessToken) -> Result<(), Error> {
        self.delete_for_user(personal_access_token.user_id, &personal_access_token.name)
            .await
    }
}

fn get_key(token_hash: &str) -> String {
    format!("{}:token:{}", KEY_PREFIX, token_hash)
}

fn get_name_key(user_id: UserId, name: &str) -> String {
    format!("{}:user:{}:{}", KEY_PREFIX, user_id, name)
}
