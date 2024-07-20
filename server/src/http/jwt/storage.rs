use crate::http::jwt::json_web_token::RevokedAccessToken;
use crate::streaming::persistence::persister::Persister;
use crate::streaming::utils::file;
use anyhow::Context;
use bytes::{BufMut, BytesMut};
use iggy::error::IggyError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tracing::info;

#[derive(Debug)]
pub struct TokenStorage {
    persister: Arc<dyn Persister>,
    path: String,
}

impl TokenStorage {
    pub fn new(persister: Arc<dyn Persister>, path: &str) -> Self {
        Self {
            persister,
            path: path.to_owned(),
        }
    }

    pub async fn load_all_revoked_access_tokens(
        &self,
    ) -> Result<Vec<RevokedAccessToken>, IggyError> {
        let file = file::open(&self.path).await;
        if file.is_err() {
            info!("No revoked access tokens found to load.");
            return Ok(vec![]);
        }

        info!("Loading revoked access tokens from: {}", self.path);
        let mut file = file.unwrap();
        let file_size = file.metadata().await?.len() as usize;
        let mut buffer = BytesMut::with_capacity(file_size);
        buffer.put_bytes(0, file_size);
        file.read_exact(&mut buffer).await?;

        let tokens: HashMap<String, u64> = bincode::deserialize(&buffer)
            .with_context(|| "Failed to deserialize revoked access tokens")
            .map_err(IggyError::CannotDeserializeResource)?;

        let tokens = tokens
            .into_iter()
            .map(|(id, expiry)| RevokedAccessToken { id, expiry })
            .collect::<Vec<RevokedAccessToken>>();

        info!("Loaded {} revoked access tokens", tokens.len());
        Ok(tokens)
    }

    pub async fn save_revoked_access_token(
        &self,
        token: &RevokedAccessToken,
    ) -> Result<(), IggyError> {
        let tokens = self.load_all_revoked_access_tokens().await?;
        let mut map = tokens
            .into_iter()
            .map(|token| (token.id, token.expiry))
            .collect::<HashMap<_, _>>();
        map.insert(token.id.to_owned(), token.expiry);
        let bytes = bincode::serialize(&map)
            .with_context(|| "Failed to serialize revoked access tokens")
            .map_err(IggyError::CannotSerializeResource)?;
        self.persister.overwrite(&self.path, &bytes).await?;
        Ok(())
    }

    pub async fn delete_revoked_access_tokens(&self, id: &[String]) -> Result<(), IggyError> {
        let tokens = self.load_all_revoked_access_tokens().await?;
        if tokens.is_empty() {
            return Ok(());
        }

        let mut map = tokens
            .into_iter()
            .map(|token| (token.id, token.expiry))
            .collect::<HashMap<_, _>>();
        for id in id {
            map.remove(id);
        }

        let bytes = bincode::serialize(&map)
            .with_context(|| "Failed to serialize revoked access tokens")
            .map_err(IggyError::CannotSerializeResource)?;
        self.persister.overwrite(&self.path, &bytes).await?;
        Ok(())
    }
}
