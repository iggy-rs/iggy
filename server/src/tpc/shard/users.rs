use crate::streaming::session::Session;
use crate::streaming::storage::{Storage, UserStorage};
use crate::streaming::users::user::User;
use crate::streaming::utils::crypto;
use iggy::error::IggyError;
use iggy::identifier::{IdKind, Identifier};
use iggy::locking::IggySharedMutFn;
use iggy::models::permissions::Permissions;
use iggy::models::user_status::UserStatus;
use iggy::users::defaults::*;
use iggy::utils::text;
use iggy::utils::text::IggyStringUtils;
use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{error, info, warn};

use super::shard::IggyShard;

static USER_ID: AtomicU32 = AtomicU32::new(1);
const MAX_USERS: usize = u32::MAX as usize;

impl IggyShard {
    pub(crate) async fn load_users(&mut self) -> Result<(), IggyError> {
        info!("Loading users...");
        let mut users = self.storage.user.load_all().await?;
        if users.is_empty() {
            info!("No users found, creating the root user...");
            let root = Self::create_root_user();
            self.storage.user.save(&root).await?;
            info!("Created the root user.");
            users = self.storage.user.load_all().await?;
        }

        let users_count = users.len();
        let current_user_id = users.iter().map(|user| user.id).max().unwrap_or(1);
        USER_ID.store(current_user_id + 1, Ordering::SeqCst);
        self.permissioner.borrow_mut().init(users);
        self.metrics.increment_users(users_count as u32);
        info!("Initialized {} user(s).", users_count);
        Ok(())
    }

    fn create_root_user() -> User {
        let username = env::var("IGGY_ROOT_USERNAME");
        let password = env::var("IGGY_ROOT_PASSWORD");
        if (username.is_ok() && password.is_err()) || (username.is_err() && password.is_ok()) {
            panic!("When providing the custom root user credentials, both username and password must be set.");
        }
        if username.is_ok() && password.is_ok() {
            info!("Using the custom root user credentials.");
        } else {
            info!("Using the default root user credentials.");
        }

        let username = username.unwrap_or(DEFAULT_ROOT_USERNAME.to_string());
        let password = password.unwrap_or(DEFAULT_ROOT_PASSWORD.to_string());
        if username.is_empty() || password.is_empty() {
            panic!("Root user credentials are not set.");
        }
        if username.len() < MIN_USERNAME_LENGTH {
            panic!("Root username is too short.");
        }
        if username.len() > MAX_USERNAME_LENGTH {
            panic!("Root username is too long.");
        }
        if password.len() < MIN_PASSWORD_LENGTH {
            panic!("Root password is too short.");
        }
        if password.len() > MAX_PASSWORD_LENGTH {
            panic!("Root password is too long.");
        }

        User::root(username, password)
    }

    pub async fn find_user(&self, client_id: u32, user_id: &Identifier) -> Result<User, IggyError> {
        let session_user_id = self.ensure_authenticated(client_id)?;
        let user = self.get_user(user_id).await?;
        if user.id != session_user_id {
            self.permissioner.borrow().get_user(session_user_id)?;
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

    pub async fn get_users(&self, client_id: u32) -> Result<Vec<User>, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        self.permissioner.borrow().get_users(user_id)?;
        self.storage.user.load_all().await
    }

    pub async fn create_user(
        &self,
        client_id: u32,
        username: String,
        password: String,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        {
            let permissioner = self.permissioner.borrow();
            permissioner.create_user(user_id)?;
        }

        let username = username.to_lowercase_non_whitespace();
        if self.storage.user.load_by_username(&username).await.is_ok() {
            error!("User: {username} already exists.");
            return Err(IggyError::UserAlreadyExists);
        }

        if self.storage.user.load_all().await?.len() > MAX_USERS {
            error!("Available users limit reached.");
            return Err(IggyError::UsersLimitReached);
        }

        let user_id = USER_ID.fetch_add(1, Ordering::SeqCst);
        info!("Creating user: {username} with ID: {user_id}...");
        let user = User::new(user_id, username.clone(), password, status, permissions);
        self.storage.user.save(&user).await?;
        self.permissioner
            .borrow_mut()
            .init_permissions_for_user(user);
        info!("Created user: {username} with ID: {user_id}.");
        self.metrics.increment_users(1);
        Ok(())
    }

    pub async fn delete_user(
        &self,
        client_id: u32,
        user_id: &Identifier,
    ) -> Result<User, IggyError> {
        let session_user_id = self.ensure_authenticated(client_id)?;
        {
            let permissioner = self.permissioner.borrow();
            permissioner.delete_user(session_user_id)?;
        }
        let user = self.get_user(user_id).await?;
        if user.is_root() {
            error!("Cannot delete the root user.");
            return Err(IggyError::CannotDeleteUser(user.id));
        }

        info!("Deleting user: {} with ID: {user_id}...", user.username);
        self.storage.user.delete(&user).await?;
        // TODO - those have to be broadcasted to other shards.
        self.permissioner
            .borrow_mut()
            .delete_permissions_for_user(user.id);
        self.client_manager
            .borrow_mut()
            .delete_clients_for_user(user.id)
            .await?;
        info!("Deleted user: {} with ID: {user_id}.", user.username);
        self.metrics.decrement_users(1);
        Ok(user)
    }

    pub async fn update_user(
        &self,
        client_id: u32,
        user_id: &Identifier,
        username: Option<String>,
        status: Option<UserStatus>,
    ) -> Result<User, IggyError> {
        let session_user_id = self.ensure_authenticated(client_id)?;
        self.permissioner.borrow().update_user(session_user_id)?;
        let mut user = self.get_user(user_id).await?;
        if let Some(username) = username {
            let username = username.to_lowercase_non_whitespace();
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
        &self,
        client_id: u32,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        let session_user_id = self.ensure_authenticated(client_id)?;
        {
            let permissioner = self.permissioner.borrow();
            permissioner.update_permissions(session_user_id)?;
        }
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
        self.permissioner
            .borrow_mut()
            .update_permissions_for_user(user);
        info!(
            "Updated permissions for user: {} with ID: {user_id}.",
            username
        );
        Ok(())
    }

    pub async fn change_password(
        &self,
        client_id: u32,
        user_id: &Identifier,
        current_password: String,
        new_password: String,
    ) -> Result<(), IggyError> {
        let session_user_id = self.ensure_authenticated(client_id)?;
        let mut user = self.get_user(user_id).await?;
        if user.id != session_user_id {
            self.permissioner
                .borrow()
                .change_password(session_user_id)?;
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
        username: String,
        password: String,
        client_id: u32,
    ) -> Result<User, IggyError> {
        self.login_user_with_credentials(username, Some(password), client_id)
            .await
    }

    pub async fn login_user_with_credentials(
        &self,
        username: String,
        password: Option<String>,
        client_id: u32,
    ) -> Result<User, IggyError> {
        let user = match self.storage.user.load_by_username(&username).await {
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
        if self.ensure_authenticated(client_id).is_err() {
            return Ok(user);
        }

        // TODO - maybe this can be solved better ?
        let active_sessions = self.active_sessions.borrow();
        let session = active_sessions
            .iter()
            .find(|s| s.client_id == client_id)
            .expect(format!("At this point session for {}, should exist.", client_id).as_str());
        if session.is_authenticated().is_ok() {
            warn!(
                "User: {} with ID: {} was already authenticated, removing the previous session...",
                user.username,
                session.get_user_id()
            );
            self.logout_user(session.client_id).await?;
        }

        session.set_user_id(user.id);
        let mut client_manager = self.client_manager.borrow_mut();
        client_manager
            .set_user_id(session.client_id, user.id)
            .await?;
        Ok(user)
    }

    pub async fn logout_user(&self, client_id: u32) -> Result<(), IggyError> {
        let active_sessions = self.active_sessions.borrow();
        let session = active_sessions
            .iter()
            .find(|s| s.client_id == client_id)
            .expect("Session for client_id should exist.");
        let user_id = session.get_user_id();

        let user = self.get_user(&Identifier::numeric(user_id)?).await?;
        info!(
            "Logging out user: {} with ID: {}...",
            user.username, user.id
        );
        // TODO - this has to be broadcasted to other shards.
        if user_id > 0 {
            let mut client_manager = self.client_manager.borrow_mut();
            client_manager.clear_user_id(client_id).await?;
            session.clear_user_id();
            info!("Cleared user ID: {} for client: {}.", user.id, client_id);
        }
        info!("Logged out user: {} with ID: {}.", user.username, user.id);
        Ok(())
    }
}
