use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use anyhow::Context;
use iggy::error::IggyError;
use iggy::models::user_info::UserId;
use sled::Db;
use std::str::from_utf8;
use std::sync::Arc;

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

impl FilePersonalAccessTokenStorage {
    async fn load(&self, personal_access_token: &mut PersonalAccessToken) -> Result<(), IggyError> {
        self.load_by_name(personal_access_token.user_id, &personal_access_token.name)
            .await?;
        Ok(())
    }

    async fn load_all(&self) -> Result<Vec<PersonalAccessToken>, IggyError> {
        let mut personal_access_tokens = Vec::new();
        for data in self.db.scan_prefix(format!("{}:token:", KEY_PREFIX)) {
            let personal_access_token = match data
                .with_context(|| format!("Failed to load personal access token, when searching by key: {}", KEY_PREFIX)){
                Ok((_, value)) => match rmp_serde::from_slice::<PersonalAccessToken>(&value)
                    .with_context(|| format!("Failed to deserialize personal access token, when searching by key: {}", KEY_PREFIX)){
                    Ok(personal_access_token) => personal_access_token,
                    Err(err) => {
                        return Err(IggyError::CannotDeserializeResource(err));
                    }
                },
                Err(err) => {
                    return Err(IggyError::CannotLoadResource(err));
                }
            };
            personal_access_tokens.push(personal_access_token);
        }

        Ok(personal_access_tokens)
    }

    async fn load_for_user(&self, user_id: UserId) -> Result<Vec<PersonalAccessToken>, IggyError> {
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
                    return Err(IggyError::CannotLoadResource(err));
                }
            };
        }

        Ok(personal_access_tokens)
    }

    async fn load_by_token(&self, token: &str) -> Result<PersonalAccessToken, IggyError> {
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
                        Err(IggyError::CannotDeserializeResource(err))
                    } else {
                        Ok(personal_access_token.unwrap())
                    }
                } else {
                    Err(IggyError::ResourceNotFound(key))
                }
            }
            Err(err) => Err(IggyError::CannotLoadResource(err)),
        };
    }

    async fn load_by_name(
        &self,
        user_id: UserId,
        name: &str,
    ) -> Result<PersonalAccessToken, IggyError> {
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
                        Err(IggyError::CannotDeserializeResource(err))
                    } else {
                        Ok(self.load_by_token(token.unwrap()).await?)
                    }
                } else {
                    Err(IggyError::ResourceNotFound(key))
                }
            }
            Err(err) => Err(IggyError::CannotLoadResource(err)),
        };
    }
}

fn get_key(token_hash: &str) -> String {
    format!("{}:token:{}", KEY_PREFIX, token_hash)
}

fn get_name_key(user_id: UserId, name: &str) -> String {
    format!("{}:user:{}:{}", KEY_PREFIX, user_id, name)
}
