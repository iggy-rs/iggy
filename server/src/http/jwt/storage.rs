use crate::http::jwt::json_web_token::RevokedAccessToken;
use crate::http::jwt::refresh_token::RefreshToken;
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
        let token_data = self.db.get(&key);
        if token_data.is_err() {
            return Err(Error::CannotLoadResource(key));
        }

        let token_data = token_data.unwrap();
        if token_data.is_none() {
            return Err(Error::CannotLoadResource(key));
        }

        let token_data = token_data.unwrap();
        let token_data = rmp_serde::from_slice::<RefreshToken>(&token_data);
        if token_data.is_err() {
            return Err(Error::CannotDeserializeResource(key));
        }

        let mut token_data = token_data.unwrap();
        token_data.token_hash = token_hash.to_string();
        Ok(token_data)
    }

    pub fn load_all_refresh_tokens(&self) -> Result<Vec<RefreshToken>, Error> {
        let mut refresh_tokens = Vec::new();
        let key = format!("{REFRESH_TOKENS_KEY_PREFIX}:");
        for data in self.db.scan_prefix(&key) {
            let token = match data {
                Ok((hash, value)) => match rmp_serde::from_slice::<RefreshToken>(&value) {
                    Ok(mut token) => {
                        token.token_hash = from_utf8(&hash)?.to_string();
                        token
                    }
                    Err(err) => {
                        error!("Cannot deserialize refresh token. Error: {}", err);
                        return Err(Error::CannotDeserializeResource(key.to_string()));
                    }
                },
                Err(err) => {
                    error!("Cannot load refresh token. Error: {}", err);
                    return Err(Error::CannotLoadResource(key.to_string()));
                }
            };
            refresh_tokens.push(token);
        }
        info!("Loaded {} refresh tokens", refresh_tokens.len());
        Ok(refresh_tokens)
    }

    pub fn load_all_revoked_access_tokens(&self) -> Result<Vec<RevokedAccessToken>, Error> {
        let mut revoked_tokens = Vec::new();
        let key = format!("{REVOKED_ACCESS_TOKENS_KEY_PREFIX}:");
        for data in self.db.scan_prefix(&key) {
            let token = match data {
                Ok((_, value)) => match rmp_serde::from_slice::<RevokedAccessToken>(&value) {
                    Ok(token) => token,
                    Err(err) => {
                        error!("Cannot deserialize revoked access token. Error: {}", err);
                        return Err(Error::CannotDeserializeResource(key.to_string()));
                    }
                },
                Err(err) => {
                    error!("Cannot load revoked access token. Error: {}", err);
                    return Err(Error::CannotLoadResource(key.to_string()));
                }
            };
            revoked_tokens.push(token);
        }
        info!("Loaded {} revoked access tokens", revoked_tokens.len());
        Ok(revoked_tokens)
    }

    pub fn save_revoked_access_token(&self, token: &RevokedAccessToken) -> Result<(), Error> {
        let key = Self::get_revoked_token_key(&token.id);
        match rmp_serde::to_vec(&token) {
            Ok(data) => {
                if let Err(err) = self.db.insert(&key, data) {
                    error!("Cannot save revoked access token. Error: {err}");
                    return Err(Error::CannotSaveResource(key.to_string()));
                }
            }
            Err(err) => {
                error!("Cannot serialize revoked access token. Error: {err}");
                return Err(Error::CannotSerializeResource(key));
            }
        }
        Ok(())
    }

    pub fn save_refresh_token(&self, token: &RefreshToken) -> Result<(), Error> {
        let key = Self::get_refresh_token_key(&token.token_hash);
        match rmp_serde::to_vec(&token) {
            Ok(data) => {
                if let Err(err) = self.db.insert(&key, data) {
                    error!("Cannot save refresh token. Error: {err}");
                    return Err(Error::CannotSaveResource(key.to_string()));
                }
            }
            Err(err) => {
                error!("Cannot serialize refresh token. Error: {err}");
                return Err(Error::CannotSerializeResource(key));
            }
        }
        Ok(())
    }

    pub fn delete_revoked_access_token(&self, id: &str) -> Result<(), Error> {
        let key = Self::get_revoked_token_key(id);
        if let Err(err) = self.db.remove(&key) {
            error!("Cannot delete revoked access token. Error: {err}");
            return Err(Error::CannotDeleteResource(key.to_string()));
        }
        Ok(())
    }

    pub fn delete_refresh_token(&self, token_hash: &str) -> Result<(), Error> {
        let key = Self::get_refresh_token_key(token_hash);
        if let Err(err) = self.db.remove(&key) {
            error!("Cannot delete refresh token. Error: {err}");
            return Err(Error::CannotDeleteResource(key.to_string()));
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
