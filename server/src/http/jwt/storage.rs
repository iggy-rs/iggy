use crate::http::jwt::json_web_token::RevokedAccessToken;
use crate::http::jwt::refresh_token::RefreshToken;
use anyhow::Context;
use iggy::error::Error;
use sled::Db;
use std::str::from_utf8;
use std::sync::Arc;
use tracing::{error, info};

const REVOKED_ACCESS_TOKENS_KEY_PREFIX: &str = "revoked_access_token";
const REFRESH_TOKENS_KEY_PREFIX: &str = "refresh_token";

#[derive(Debug)]
pub struct TokenStorage {
    db: Arc<Db>,
}

impl TokenStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    pub fn load_refresh_token(&self, token_hash: &str) -> Result<RefreshToken, Error> {
        let key = Self::get_refresh_token_key(token_hash);
        let token_data = self
            .db
            .get(&key)
            .with_context(|| format!("Failed to load refresh token, key: {}", key));
        if let Err(err) = token_data {
            return Err(Error::CannotLoadResource(err));
        }

        let token_data = token_data.unwrap();
        if token_data.is_none() {
            return Err(Error::ResourceNotFound(key));
        }

        let token_data = token_data.unwrap();
        let token_data = rmp_serde::from_slice::<RefreshToken>(&token_data)
            .with_context(|| format!("Failed to deserialize refresh token, key: {}", key));
        if let Err(err) = token_data {
            return Err(Error::CannotDeserializeResource(err));
        }

        let mut token_data = token_data.unwrap();
        token_data.token_hash = token_hash.to_string();
        Ok(token_data)
    }

    pub fn load_all_refresh_tokens(&self) -> Result<Vec<RefreshToken>, Error> {
        let key = format!("{REFRESH_TOKENS_KEY_PREFIX}:");
        let refresh_tokens: Result<Vec<RefreshToken>, Error> = self
            .db
            .scan_prefix(&key)
            .map(|data| {
                let (hash, value) = data
                    .with_context(|| {
                        format!(
                            "Failed to load refresh token, when searching by key: {}",
                            key
                        )
                    })
                    .map_err(Error::CannotLoadResource)?;

                let mut token = rmp_serde::from_slice::<RefreshToken>(&value)
                    .with_context(|| {
                        format!(
                            "Failed to deserialize refresh token, when searching by key: {}",
                            key
                        )
                    })
                    .map_err(Error::CannotDeserializeResource)?;

                token.token_hash = from_utf8(&hash)
                    .with_context(|| "Failed to convert hash to UTF-8 string")
                    .map_err(Error::CannotDeserializeResource)?
                    .to_string();
                Ok(token)
            })
            .collect();

        let refresh_tokens = refresh_tokens?;
        info!("Loaded {} refresh tokens", refresh_tokens.len());
        Ok(refresh_tokens)
    }

    pub fn load_all_revoked_access_tokens(&self) -> Result<Vec<RevokedAccessToken>, Error> {
        let key = format!("{REVOKED_ACCESS_TOKENS_KEY_PREFIX}:");
        let revoked_tokens: Result<Vec<RevokedAccessToken>, Error> = self
            .db
            .scan_prefix(&key)
            .map(|data| {
                let (_, value) = data
                    .with_context(|| {
                        format!(
                            "Failed to load invoked refresh token, when searching by key: {}",
                            key
                        )
                    })
                    .map_err(Error::CannotLoadResource)?;

                let token = rmp_serde::from_slice::<RevokedAccessToken>(&value)
                    .with_context(|| {
                        format!(
                            "Failed to deserialize revoked access token, when searching by key: {}",
                            key
                        )
                    })
                    .map_err(Error::CannotDeserializeResource)?;
                Ok(token)
            })
            .collect();

        let revoked_tokens = revoked_tokens?;
        info!("Loaded {} revoked access tokens", revoked_tokens.len());
        Ok(revoked_tokens)
    }

    pub fn save_revoked_access_token(&self, token: &RevokedAccessToken) -> Result<(), Error> {
        let key = Self::get_revoked_token_key(&token.id);
        match rmp_serde::to_vec(&token)
            .with_context(|| format!("Failed to serialize revoked access token, key: {}", key))
        {
            Ok(data) => {
                if let Err(err) = self
                    .db
                    .insert(&key, data)
                    .with_context(|| "Failed to save revoked access token")
                {
                    return Err(Error::CannotSaveResource(err));
                }
            }
            Err(err) => {
                return Err(Error::CannotSerializeResource(err));
            }
        }
        Ok(())
    }

    pub fn save_refresh_token(&self, token: &RefreshToken) -> Result<(), Error> {
        let key = Self::get_refresh_token_key(&token.token_hash);
        match rmp_serde::to_vec(&token)
            .with_context(|| format!("Failed to serialize refresh token, key: {}", key))
        {
            Ok(data) => {
                if let Err(err) = self
                    .db
                    .insert(&key, data)
                    .with_context(|| format!("Failed to save refresh token, key: {}", key))
                {
                    return Err(Error::CannotSaveResource(err));
                }
            }
            Err(err) => {
                return Err(Error::CannotSerializeResource(err));
            }
        }
        Ok(())
    }

    pub fn delete_revoked_access_token(&self, id: &str) -> Result<(), Error> {
        let key = Self::get_revoked_token_key(id);
        if let Err(err) = self
            .db
            .remove(&key)
            .with_context(|| format!("Failed to delete revoked access token, key: {}", key))
        {
            return Err(Error::CannotDeleteResource(err));
        }
        Ok(())
    }

    pub fn delete_refresh_token(&self, token_hash: &str) -> Result<(), Error> {
        let key = Self::get_refresh_token_key(token_hash);
        if let Err(err) = self
            .db
            .remove(&key)
            .with_context(|| format!("Failed to delete refresh token, key: {}", key))
        {
            error!("Cannot delete refresh token. Error: {err}");
            return Err(Error::CannotDeleteResource(err));
        }
        Ok(())
    }

    fn get_revoked_token_key(id: &str) -> String {
        format!("{REVOKED_ACCESS_TOKENS_KEY_PREFIX}:{id}")
    }

    fn get_refresh_token_key(token_hash: &str) -> String {
        format!("{REFRESH_TOKENS_KEY_PREFIX}:{token_hash}")
    }
}
