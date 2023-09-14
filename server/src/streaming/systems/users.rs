use crate::streaming::systems::system::System;
use crate::streaming::users::user::User;
use crate::streaming::utils::crypto;
use iggy::error::Error;
use tracing::info;
use tracing::log::error;

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
        self.permissioner.init(users);
        if self.config.user.authorization_enabled {
            self.permissioner.enable()
        }
        info!("Initialized {} user(s).", users_count);
        Ok(())
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
        if !crypto::verify_password(password, &user.password) {
            return Err(Error::InvalidCredentials);
        }
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
