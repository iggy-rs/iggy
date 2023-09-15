use crate::streaming::systems::system::System;
use crate::streaming::users::user::User;
use crate::streaming::utils::crypto;
use iggy::error::Error;
use iggy::models::permissions::Permissions;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::log::error;
use tracing::{info, warn};

static USER_ID: AtomicU32 = AtomicU32::new(1);

impl System {
    pub(crate) async fn load_users(&mut self) -> Result<(), Error> {
        info!("Loading users...");
        let mut users = self.storage.user.load_all().await?;
        if users.is_empty() {
            info!("No users found, creating the root user...");
            let root = User::root();
            self.storage.user.save(&root).await?;
            info!("Created the root user.");
            users = self.storage.user.load_all().await?;
        }

        let users_count = users.len();
        let current_user_id = users.iter().map(|user| user.id).max().unwrap_or(1);
        USER_ID.store(current_user_id + 1, Ordering::SeqCst);
        self.permissioner.init(users);
        if self.config.user.authorization_enabled {
            self.permissioner.enable()
        }
        info!("Initialized {} user(s).", users_count);
        Ok(())
    }

    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        permissions: Option<Permissions>,
    ) -> Result<User, Error> {
        if self.storage.user.load_by_username(username).await.is_ok() {
            error!("User: {username} already exists.");
            return Err(Error::UserAlreadyExists);
        }
        // TODO: What if reach the max value and there are some deleted accounts (not used IDs)?
        let user_id = USER_ID.fetch_add(1, Ordering::SeqCst);
        info!("Creating user: {username} with ID: {user_id}...");
        let user = User::new(user_id, username, password, permissions);
        self.storage.user.save(&user).await?;
        info!("Created user: {username} with ID: {user_id}.");
        Ok(user)
    }

    pub async fn login_user(&self, username: &str, password: &str) -> Result<User, Error> {
        info!("Logging in user: {username}...");
        let user = match self.storage.user.load_by_username(username).await {
            Ok(user) => user,
            Err(_) => {
                error!("Cannot login user: {username}.");
                return Err(Error::InvalidCredentials);
            }
        };
        if !user.is_active() {
            warn!("User: {username} is inactive.");
            return Err(Error::UserInactive);
        }
        if !crypto::verify_password(password, &user.password) {
            return Err(Error::InvalidCredentials);
        }
        // TODO: Store the currently authenticated users in memory. Keep in mind that the single user account might be used by multiple clients.
        info!("Logged in user: {username}.");
        Ok(user)
    }

    pub async fn logout_user(&self, user_id: u32) -> Result<(), Error> {
        if user_id == 0 {
            return Err(Error::InvalidCredentials);
        }
        info!("Logging out user: {user_id}...");
        // TODO: Implement user logout on the system level
        info!("Logged out user: {user_id}.");
        Ok(())
    }
}
