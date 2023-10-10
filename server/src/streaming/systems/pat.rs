use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use crate::streaming::users::pat::PersonalAccessToken;
use iggy::error::Error;
use iggy::utils::text;
use tracing::{error, info};

const MAX_PERSONAL_ACCESS_TOKENS: u32 = 100;

impl System {
    pub async fn get_personal_access_tokens(
        &self,
        session: &Session,
    ) -> Result<Vec<PersonalAccessToken>, Error> {
        self.ensure_authenticated(session)?;
        let user_id = session.user_id;
        info!("Loading PATs for user with ID: {user_id}...",);
        let pats = self.storage.user.load_pats_for_user(user_id).await?;
        info!(
            "Loaded {count} PAT(s) for user with ID: {user_id}.",
            count = pats.len(),
        );
        Ok(pats)
    }

    pub async fn create_personal_access_token(
        &self,
        session: &Session,
        name: &str,
        expiry: Option<u32>,
    ) -> Result<String, Error> {
        self.ensure_authenticated(session)?;
        let user_id = session.user_id;
        let name = text::to_lowercase_non_whitespace(name);
        let pats = self.storage.user.load_pats_for_user(user_id).await?;
        if pats.len() as u32 >= MAX_PERSONAL_ACCESS_TOKENS {
            error!(
                "User with ID: {} has reached the maximum number of PATs: {}.",
                user_id, MAX_PERSONAL_ACCESS_TOKENS,
            );
            return Err(Error::PatLimitReached(user_id, MAX_PERSONAL_ACCESS_TOKENS));
        }

        if pats.iter().any(|pat| pat.name == name) {
            error!("PAT: {name} for user with ID: {user_id} already exists.");
            return Err(Error::PatAlreadyExists(name, user_id));
        }

        info!("Creating PAT: {name} for user with ID: {user_id}...");
        let (pat, token) = PersonalAccessToken::new(user_id, &name, expiry);
        self.storage.user.save_pat(&pat).await?;
        info!("Created PAT: {name} for user with ID: {user_id}.");
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
        info!("Deleting PAT: {name} for user with ID: {user_id}...");
        self.storage.user.delete_pat(user_id, &name).await?;
        info!("Deleted PAT: {name} for user with ID: {user_id}.");
        Ok(())
    }
}
