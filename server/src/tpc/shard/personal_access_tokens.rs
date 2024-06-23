use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::session::Session;
use crate::streaming::storage::{PersonalAccessTokenStorage, Storage, UserStorage};
use crate::streaming::users::user::User;
use iggy::error::IggyError;
use iggy::utils::text;
use iggy::utils::text::IggyStringUtils;
use iggy::utils::timestamp::IggyTimestamp;
use tracing::{error, info};

use super::shard::IggyShard;

impl IggyShard {
    pub async fn get_personal_access_tokens(
        &self,
        client_id: u32,
    ) -> Result<Vec<PersonalAccessToken>, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        info!("Loading personal access tokens for user with ID: {user_id}...",);
        let personal_access_tokens = self
            .storage
            .personal_access_token
            .load_for_user(user_id)
            .await?;
        info!(
            "Loaded {count} personal access tokens for user with ID: {user_id}.",
            count = personal_access_tokens.len(),
        );
        Ok(personal_access_tokens)
    }

    pub async fn create_personal_access_token(
        &self,
        client_id: u32,
        name: String,
        expiry: Option<u32>,
    ) -> Result<String, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let max_token_per_user = self.config.personal_access_token.max_tokens_per_user;
        let name = name.to_lowercase_non_whitespace();
        let personal_access_tokens = self
            .storage
            .personal_access_token
            .load_for_user(user_id)
            .await?;
        if personal_access_tokens.len() as u32 >= max_token_per_user {
            error!(
                "User with ID: {} has reached the maximum number of personal access tokens: {}.",
                user_id, max_token_per_user,
            );
            return Err(IggyError::PersonalAccessTokensLimitReached(
                user_id,
                max_token_per_user,
            ));
        }

        if personal_access_tokens
            .iter()
            .any(|personal_access_token| personal_access_token.name == name)
        {
            error!("Personal access token: {name} for user with ID: {user_id} already exists.");
            return Err(IggyError::PersonalAccessTokenAlreadyExists(name, user_id));
        }

        info!("Creating personal access token: {name} for user with ID: {user_id}...");
        let (personal_access_token, token) = PersonalAccessToken::new(
            user_id,
            name.clone(),
            IggyTimestamp::now().to_micros(),
            expiry,
        );
        self.storage
            .personal_access_token
            .save(&personal_access_token)
            .await?;
        info!("Created personal access token: {name} for user with ID: {user_id}.");
        Ok(token)
    }

    pub async fn delete_personal_access_token(
        &self,
        client_id: u32,
        name: String,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let name = name.to_lowercase_non_whitespace();
        info!("Deleting personal access token: {name} for user with ID: {user_id}...");
        self.storage
            .personal_access_token
            .delete_for_user(user_id, &name)
            .await?;
        info!("Deleted personal access token: {name} for user with ID: {user_id}.");
        Ok(())
    }

    pub async fn login_with_personal_access_token(
        &self,
        client_id: u32,
        token: String,
    ) -> Result<User, IggyError> {
        let token_hash = PersonalAccessToken::hash_token(token);
        let personal_access_token = self
            .storage
            .personal_access_token
            .load_by_token(&token_hash)
            .await?;
        if personal_access_token.is_expired(IggyTimestamp::now().to_micros()) {
            error!(
                "Personal access token: {} for user with ID: {} has expired.",
                personal_access_token.name, personal_access_token.user_id
            );
            return Err(IggyError::PersonalAccessTokenExpired(
                personal_access_token.name,
                personal_access_token.user_id,
            ));
        }

        let user = self
            .storage
            .user
            .load_by_id(personal_access_token.user_id)
            .await?;
        self.login_user_with_credentials(user.username, None, client_id)
            .await
    }
}
