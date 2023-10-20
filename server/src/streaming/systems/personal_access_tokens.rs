use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use crate::streaming::users::user::User;
use iggy::error::Error;
use iggy::utils::text;
use iggy::utils::timestamp::TimeStamp;
use tracing::{error, info};

impl System {
    pub async fn get_personal_access_tokens(
        &self,
        session: &Session,
    ) -> Result<Vec<PersonalAccessToken>, Error> {
        self.ensure_authenticated(session)?;
        let user_id = session.user_id;
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
        session: &Session,
        name: &str,
        expiry: Option<u32>,
    ) -> Result<String, Error> {
        self.ensure_authenticated(session)?;
        let user_id = session.user_id;
        let max_token_per_user = self.personal_access_token.max_tokens_per_user;
        let name = text::to_lowercase_non_whitespace(name);
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
            return Err(Error::PersonalAccessTokensLimitReached(
                user_id,
                max_token_per_user,
            ));
        }

        if personal_access_tokens
            .iter()
            .any(|personal_access_token| personal_access_token.name == name)
        {
            error!("Personal access token: {name} for user with ID: {user_id} already exists.");
            return Err(Error::PersonalAccessTokenAlreadyExists(name, user_id));
        }

        info!("Creating personal access token: {name} for user with ID: {user_id}...");
        let (personal_access_token, token) =
            PersonalAccessToken::new(user_id, &name, TimeStamp::now().to_micros(), expiry);
        self.storage
            .personal_access_token
            .save(&personal_access_token)
            .await?;
        info!("Created personal access token: {name} for user with ID: {user_id}.");
        Ok(token)
    }

    pub async fn delete_personal_access_token(
        &self,
        session: &Session,
        name: &str,
    ) -> Result<(), Error> {
        self.ensure_authenticated(session)?;
        let user_id = session.user_id;
        let name = text::to_lowercase_non_whitespace(name);
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
        token: &str,
        session: Option<&mut Session>,
    ) -> Result<User, Error> {
        let token_hash = PersonalAccessToken::hash_token(token);
        let personal_access_token = self
            .storage
            .personal_access_token
            .load_by_token(&token_hash)
            .await?;
        if personal_access_token.is_expired(TimeStamp::now().to_micros()) {
            error!(
                "Personal access token: {} for user with ID: {} has expired.",
                personal_access_token.name, personal_access_token.user_id
            );
            return Err(Error::PersonalAccessTokenExpired(
                personal_access_token.name,
                personal_access_token.user_id,
            ));
        }

        let user = self
            .storage
            .user
            .load_by_id(personal_access_token.user_id)
            .await?;
        self.login_user_with_credentials(&user.username, None, session)
            .await
    }
}
