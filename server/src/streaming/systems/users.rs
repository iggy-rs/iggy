use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use crate::streaming::users::user::User;
use crate::streaming::utils::crypto;
use iggy::error::IggyError;
use iggy::identifier::{IdKind, Identifier};
use iggy::locking::IggySharedMutFn;
use iggy::models::permissions::Permissions;
use iggy::models::user_status::UserStatus;
use iggy::utils::text;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::log::error;
use tracing::{info, warn};

static USER_ID: AtomicU32 = AtomicU32::new(1);

impl System {
    pub(crate) async fn load_users(&mut self) -> Result<(), IggyError> {
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
        info!("Initialized {} user(s).", users_count);
        Ok(())
    }

    pub async fn find_user(
        &self,
        session: &Session,
        user_id: &Identifier,
    ) -> Result<User, IggyError> {
        self.ensure_authenticated(session)?;
        let user = self.get_user(user_id).await?;
        let session_user_id = session.get_user_id();
        if user.id != session_user_id {
            self.permissioner.get_user(session_user_id)?;
        }

        Ok(user)
    }

    pub async fn get_user(&self, user_id: &Identifier) -> Result<User, IggyError> {
        Ok(match user_id.kind {
            IdKind::Numeric => {
                self.storage
                    .user
                    .load_by_id(user_id.get_u32_value()?)
                    .await?
            }
            IdKind::String => {
                self.storage
                    .user
                    .load_by_username(&user_id.get_cow_str_value()?)
                    .await?
            }
        })
    }

    pub async fn get_users(&self, session: &Session) -> Result<Vec<User>, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner.get_users(session.get_user_id())?;
        self.storage.user.load_all().await
    }

    pub async fn create_user(
        &mut self,
        session: &Session,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner.create_user(session.get_user_id())?;
        let username = text::to_lowercase_non_whitespace(username);
        if self.storage.user.load_by_username(&username).await.is_ok() {
            error!("User: {username} already exists.");
            return Err(IggyError::UserAlreadyExists);
        }
        let user_id = USER_ID.fetch_add(1, Ordering::SeqCst);
        info!("Creating user: {username} with ID: {user_id}...");
        let user = User::new(user_id, &username, password, status, permissions);
        self.storage.user.save(&user).await?;
        self.permissioner.init_permissions_for_user(user);
        info!("Created user: {username} with ID: {user_id}.");
        self.metrics.increment_users(1);
        Ok(())
    }

    pub async fn delete_user(
        &mut self,
        session: &Session,
        user_id: &Identifier,
    ) -> Result<User, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner.delete_user(session.get_user_id())?;
        let user = self.get_user(user_id).await?;
        if user.is_root() {
            error!("Cannot delete the root user.");
            return Err(IggyError::CannotDeleteUser(user.id));
        }

        info!("Deleting user: {} with ID: {user_id}...", user.username);
        self.storage.user.delete(&user).await?;
        self.permissioner.delete_permissions_for_user(user.id);
        let mut client_manager = self.client_manager.write().await;
        client_manager.delete_clients_for_user(user.id).await?;
        info!("Deleted user: {} with ID: {user_id}.", user.username);
        self.metrics.decrement_users(1);
        Ok(user)
    }

    pub async fn update_user(
        &self,
        session: &Session,
        user_id: &Identifier,
        username: Option<String>,
        status: Option<UserStatus>,
    ) -> Result<User, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner.update_user(session.get_user_id())?;
        let mut user = self.get_user(user_id).await?;
        if let Some(username) = username {
            let username = text::to_lowercase_non_whitespace(&username);
            let existing_user = self.storage.user.load_by_username(&username).await;
            if existing_user.is_ok() && existing_user.unwrap().id != user.id {
                error!("User: {username} already exists.");
                return Err(IggyError::UserAlreadyExists);
            }
            self.storage.user.delete(&user).await?;
            user.username = username;
        }

        if let Some(status) = status {
            user.status = status;
        }

        info!("Updating user: {} with ID: {}...", user.username, user.id);
        self.storage.user.save(&user).await?;
        info!("Updated user: {} with ID: {}.", user.username, user.id);
        Ok(user)
    }

    pub async fn update_permissions(
        &mut self,
        session: &Session,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .update_permissions(session.get_user_id())?;
        let mut user = self.get_user(user_id).await?;
        if user.is_root() {
            error!("Cannot change the root user permissions.");
            return Err(IggyError::CannotChangePermissions(user.id));
        }

        user.permissions = permissions;
        let username = user.username.clone();
        info!(
            "Updating permissions for user: {} with ID: {user_id}...",
            username
        );
        self.storage.user.save(&user).await?;
        self.permissioner.update_permissions_for_user(user);
        info!(
            "Updated permissions for user: {} with ID: {user_id}.",
            username
        );
        Ok(())
    }

    pub async fn change_password(
        &self,
        session: &Session,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let mut user = self.get_user(user_id).await?;
        let session_user_id = session.get_user_id();
        if user.id != session_user_id {
            self.permissioner.change_password(session_user_id)?;
        }

        if !crypto::verify_password(current_password, &user.password) {
            error!(
                "Invalid current password for user: {} with ID: {user_id}.",
                user.username
            );
            return Err(IggyError::InvalidCredentials);
        }

        info!(
            "Changing password for user: {} with ID: {user_id}...",
            user.username
        );
        user.password = crypto::hash_password(new_password);
        self.storage.user.save(&user).await?;
        info!(
            "Changed password for user: {} with ID: {user_id}.",
            user.username
        );
        Ok(())
    }

    pub async fn login_user(
        &self,
        username: &str,
        password: &str,
        session: Option<&Session>,
    ) -> Result<User, IggyError> {
        self.login_user_with_credentials(username, Some(password), session)
            .await
    }

    pub async fn login_user_with_credentials(
        &self,
        username: &str,
        password: Option<&str>,
        session: Option<&Session>,
    ) -> Result<User, IggyError> {
        let user = match self.storage.user.load_by_username(username).await {
            Ok(user) => user,
            Err(_) => {
                error!("Cannot login user: {username} (not found).");
                return Err(IggyError::InvalidCredentials);
            }
        };

        info!("Logging in user: {username} with ID: {}...", user.id);
        if !user.is_active() {
            warn!("User: {username} with ID: {} is inactive.", user.id);
            return Err(IggyError::UserInactive);
        }

        if let Some(password) = password {
            if !crypto::verify_password(password, &user.password) {
                warn!(
                    "Invalid password for user: {username} with ID: {}.",
                    user.id
                );
                return Err(IggyError::InvalidCredentials);
            }
        }

        info!("Logged in user: {username} with ID: {}.", user.id);
        if session.is_none() {
            return Ok(user);
        }

        let session = session.unwrap();
        if session.is_authenticated() {
            warn!(
                "User: {} with ID: {} was already authenticated, removing the previous session...",
                user.username,
                session.get_user_id()
            );
            self.logout_user(session).await?;
        }

        session.set_user_id(user.id);
        let mut client_manager = self.client_manager.write().await;
        client_manager
            .set_user_id(session.client_id, user.id)
            .await?;
        Ok(user)
    }

    pub async fn logout_user(&self, session: &Session) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let user = self
            .get_user(&Identifier::numeric(session.get_user_id())?)
            .await?;
        info!(
            "Logging out user: {} with ID: {}...",
            user.username, user.id
        );
        if session.client_id > 0 {
            let mut client_manager = self.client_manager.write().await;
            client_manager.clear_user_id(session.client_id).await?;
            info!(
                "Cleared user ID: {} for client: {}.",
                user.id, session.client_id
            );
        }
        info!("Logged out user: {} with ID: {}.", user.username, user.id);
        Ok(())
    }
}
