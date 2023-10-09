use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use crate::streaming::users::pat::PersonalAccessToken;
use iggy::error::Error;
use iggy::utils::text;
use tracing::{error, info};

impl System {
    pub async fn create_pat(
        &self,
        session: &Session,
        name: &str,
        expiry: Option<u32>,
    ) -> Result<String, Error> {
        self.ensure_authenticated(session)?;
        let user_id = session.user_id;
        let name = text::to_lowercase_non_whitespace(name);
        if self
            .storage
            .user
            .load_pat_by_name(user_id, &name)
            .await
            .is_ok()
        {
            error!("PAT: {name} for user with ID: {user_id} already exists.");
            return Err(Error::PatAlreadyExists(name, user_id));
        }
        info!("Creating PAT: {name} for user with ID: {user_id}...");
        let (pat, token) = PersonalAccessToken::new(user_id, &name, expiry);
        self.storage.user.save_pat(&pat).await?;
        info!("Created PAT: {name} for user with ID: {user_id}.");
        Ok(token)
    }

    pub async fn delete_pat(&self, session: &Session, name: &str) -> Result<(), Error> {
        self.ensure_authenticated(session)?;
        let user_id = session.user_id;
        let name = text::to_lowercase_non_whitespace(name);
        info!("Deleting PAT: {name} for user with ID: {user_id}...");
        self.storage.user.delete_pat(user_id, &name).await?;
        info!("Deleted PAT: {name} for user with ID: {user_id}.");
        Ok(())
    }
}
