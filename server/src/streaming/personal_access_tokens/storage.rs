use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::storage::{PersonalAccessTokenStorage, Storage};
use anyhow::Context;
use async_trait::async_trait;
use iggy::error::Error;
use iggy::models::user_info::UserId;
use sled::Db;
use std::str::from_utf8;
use std::sync::Arc;
use tracing::info;

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
            let personal_access_token = match data
                .with_context(|| format!("Failed to load personal access token, when searching by key: {}", KEY_PREFIX)){
                Ok((_, value)) => match rmp_serde::from_slice::<PersonalAccessToken>(&value)
                    .with_context(|| format!("Failed to deserialize personal access token, when searching by key: {}", KEY_PREFIX)){
                    Ok(personal_access_token) => personal_access_token,
                    Err(err) => {
                        return Err(Error::CannotDeserializeResource(err));
                    }
                },
                Err(err) => {
                    return Err(Error::CannotLoadResource(err));
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
            match data.with_context(|| {
                format!(
                    "Failed to load personal access token, for user ID: {}",
                    user_id
                )
            }) {
                Ok((_, value)) => {
                    let token = from_utf8(&value)?;
                    let personal_access_token = self.load_by_token(token).await?;
                    personal_access_tokens.push(personal_access_token);
                }
                Err(err) => {
                    return Err(Error::CannotLoadResource(err));
                }
            };
        }

        Ok(personal_access_tokens)
    }

    async fn load_by_token(&self, token: &str) -> Result<PersonalAccessToken, Error> {
        let key = get_key(token);
        return match self
            .db
            .get(&key)
            .with_context(|| format!("Failed to load personal access token, token: {}", token))
        {
            Ok(personal_access_token) => {
                if let Some(personal_access_token) = personal_access_token {
                    let personal_access_token =
                        rmp_serde::from_slice::<PersonalAccessToken>(&personal_access_token)
                            .with_context(|| "Failed to deserialize personal access token");
                    if let Err(err) = personal_access_token {
                        Err(Error::CannotDeserializeResource(err))
                    } else {
                        Ok(personal_access_token.unwrap())
                    }
                } else {
                    Err(Error::ResourceNotFound(key))
                }
            }
            Err(err) => Err(Error::CannotLoadResource(err)),
        };
    }

    async fn load_by_name(
        &self,
        user_id: UserId,
        name: &str,
    ) -> Result<PersonalAccessToken, Error> {
        let key = get_name_key(user_id, name);
        return match self.db.get(&key).with_context(|| {
            format!(
                "Failed to load personal access token, token_name: {}, user_id: {}",
                name, user_id
            )
        }) {
            Ok(token) => {
                if let Some(token) = token {
                    let token = from_utf8(&token)
                        .with_context(|| "Failed to deserialize personal access token");
                    if let Err(err) = token {
                        Err(Error::CannotDeserializeResource(err))
                    } else {
                        Ok(self.load_by_token(token.unwrap()).await?)
                    }
                } else {
                    Err(Error::ResourceNotFound(key))
                }
            }
            Err(err) => Err(Error::CannotLoadResource(err)),
        };
    }

    async fn delete_for_user(&self, user_id: UserId, name: &str) -> Result<(), Error> {
        let personal_access_token = self.load_by_name(user_id, name).await?;
        info!("Deleting personal access token with name: {name} for user with ID: {user_id}...");
        let key = get_name_key(user_id, name);
        if let Err(err) = self
            .db
            .remove(key)
            .with_context(|| "Failed to delete personal access token")
        {
            return Err(Error::CannotDeleteResource(err));
        }
        let key = get_key(&personal_access_token.token);
        if let Err(err) = self
            .db
            .remove(key)
            .with_context(|| "Failed to delete personal access token")
        {
            return Err(Error::CannotDeleteResource(err));
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
        match rmp_serde::to_vec(&personal_access_token)
            .with_context(|| "Failed to serialize personal access token")
        {
            Ok(data) => {
                if let Err(err) = self
                    .db
                    .insert(key, data)
                    .with_context(|| "Failed to save personal access token")
                {
                    return Err(Error::CannotSaveResource(err));
                }
                if let Err(err) = self
                    .db
                    .insert(
                        get_name_key(personal_access_token.user_id, &personal_access_token.name),
                        personal_access_token.token.as_bytes(),
                    )
                    .with_context(|| "Failed to save personal access token")
                {
                    return Err(Error::CannotSaveResource(err));
                }
            }
            Err(err) => {
                return Err(Error::CannotSerializeResource(err));
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
