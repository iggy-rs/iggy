use crate::state::command::EntryCommand;
use crate::state::system::UserState;
use crate::state::State;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::users::user::User;
use crate::streaming::utils::crypto;
use iggy::error::IggyError;
use iggy::identifier::{IdKind, Identifier};
use iggy::locking::IggySharedMutFn;
use iggy::models::permissions::Permissions;
use iggy::models::user_status::UserStatus;
use iggy::users::create_user::CreateUser;
use iggy::users::defaults::*;
use iggy::utils::text::IggyStringUtils;
use std::cell::RefMut;
use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{error, info, warn};

use super::shard::IggyShard;

static USER_ID: AtomicU32 = AtomicU32::new(1);
const MAX_USERS: usize = u32::MAX as usize;

impl IggyShard {
    pub(crate) async fn load_users(&self, users: Vec<UserState>) -> Result<(), IggyError> {
        info!("Loading users...");
        if users.is_empty() {
            info!("Creating the root user...");
            let root = IggyShard::create_root_user();
            let command = CreateUser {
                username: root.username.clone(),
                password: root.password.clone(),
                status: root.status,
                permissions: root.permissions.clone(),
            };
            self.state
                .apply(0, EntryCommand::CreateUser(command))
                .await?;
            self.users.borrow_mut().insert(root.id, root);
            info!("Created the root user.");
        }

        for user_state in users.into_iter() {
            let mut user = User::with_password(
                user_state.id,
                user_state.username,
                user_state.password_hash,
                user_state.status,
                user_state.permissions,
            );

            user.personal_access_tokens = user_state
                .personal_access_tokens
                .into_values()
                .map(|token| {
                    (
                        token.token_hash.clone(),
                        PersonalAccessToken::raw(
                            user_state.id,
                            &token.name,
                            &token.token_hash,
                            token.expiry_at,
                        ),
                    )
                })
                .collect();
            self.users.borrow_mut().insert(user_state.id, user);
        }

        let users_count = self.users.borrow().len();
        let users_store = self.users.borrow();
        let current_user_id = users_store.keys().max().unwrap_or(&1);
        USER_ID.store(current_user_id + 1, Ordering::SeqCst);
        self.permissioner
            .borrow_mut()
            .init(&self.users.borrow().values().collect::<Vec<_>>());
        self.metrics.increment_users(users_count as u32);
        info!("Initialized {} user(s).", users_count);
        Ok(())
    }

    pub fn create_root_user() -> User {
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
        let user = self.get_user(user_id)?;
        if user.id != session_user_id {
            self.permissioner.borrow().get_user(session_user_id)?;
        }

        Ok(user)
    }

    pub fn get_user(&self, user_id: &Identifier) -> Result<User, IggyError> {
        match user_id.kind {
            IdKind::Numeric => self
                .users
                .borrow()
                .get(&user_id.get_u32_value()?)
                .cloned()
                .ok_or(IggyError::ResourceNotFound(user_id.to_string())),
            IdKind::String => {
                let username = user_id.get_cow_str_value()?;
                self.users
                    .borrow()
                    .iter()
                    .find(|(_, user)| user.username == username)
                    .map(|(_, user)| user.clone())
                    .ok_or(IggyError::ResourceNotFound(user_id.to_string()))
            }
        }
    }

    pub fn get_user_mut(&self, user_id: &Identifier) -> Result<RefMut<'_, User>, IggyError> {
        match user_id.kind {
            IdKind::Numeric => {
                let user_id = user_id.get_u32_value()?;
                let users = self.users.borrow_mut();
                let exists = users.contains_key(&user_id);
                if !exists {
                    return Err(IggyError::ResourceNotFound(user_id.to_string()));
                }
                Ok(RefMut::map(users, |u| {
                    let user = u.get_mut(&user_id);
                    user.unwrap()
                }))
            }
            IdKind::String => {
                let username = user_id.get_cow_str_value()?;
                let users = self.users.borrow_mut();
                let exists = users
                    .iter()
                    .find(|(_, user)| user.username == username)
                    .is_some();
                if !exists {
                    return Err(IggyError::ResourceNotFound(user_id.to_string()));
                }
                Ok(RefMut::map(users, |u| {
                    let user = u
                        .iter_mut()
                        .find(|(_, user)| user.username == username)
                        .map(|(_, user)| user);
                    user.unwrap()
                }))
            }
        }
    }

    pub async fn get_users(&self, client_id: u32) -> Result<Vec<User>, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        self.permissioner.borrow().get_users(user_id)?;
        Ok(self.users.borrow().values().cloned().collect())
    }

    pub async fn create_user(
        &self,
        user_id: u32,
        username: String,
        password: String,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        {
            let permissioner = self.permissioner.borrow();
            permissioner.create_user(user_id)?;
        }

        let username = username.to_lowercase_non_whitespace();
        if self
            .users
            .borrow()
            .iter()
            .any(|(_, user)| user.username == username)
        {
            error!("User: {username} already exists.");
            return Err(IggyError::UserAlreadyExists);
        }

        if self.users.borrow().len() >= MAX_USERS {
            error!("Available users limit reached.");
            return Err(IggyError::UsersLimitReached);
        }

        let user_id = USER_ID.fetch_add(1, Ordering::SeqCst);
        info!("Creating user: {username} with ID: {user_id}...");
        let user = User::new(
            user_id,
            username.clone(),
            password,
            status,
            permissions.clone(),
        );
        self.permissioner
            .borrow_mut()
            .init_permissions_for_user(user_id, permissions);
        self.users.borrow_mut().insert(user.id, user);
        info!("Created user: {username} with ID: {user_id}.");
        self.metrics.increment_users(1);
        Ok(())
    }

    pub async fn delete_user(
        &self,
        user_id_numeric: u32,
        user_id: &Identifier,
    ) -> Result<User, IggyError> {
        let existing_user_id;
        let existing_username;
        {
            self.permissioner.borrow().delete_user(user_id_numeric)?;
            let user = self.get_user(user_id)?;
            if user.is_root() {
                error!("Cannot delete the root user.");
                return Err(IggyError::CannotDeleteUser(user.id));
            }

            existing_user_id = user.id;
            existing_username = user.username.clone();
        }

        info!("Deleting user: {existing_username} with ID: {user_id}...");
        let user = self
            .users
            .borrow_mut()
            .remove(&existing_user_id)
            .ok_or(IggyError::ResourceNotFound(user_id.to_string()))?;
        self.permissioner
            .borrow_mut()
            .delete_permissions_for_user(existing_user_id);
        self.client_manager
            .borrow_mut()
            .delete_clients_for_user(existing_user_id)
            .await?;
        info!("Deleted user: {existing_username} with ID: {user_id}.");
        self.metrics.decrement_users(1);
        Ok(user)
    }

    pub async fn update_user(
        &self,
        user_id_numeric: u32,
        user_id: &Identifier,
        username: Option<String>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        self.permissioner.borrow().update_user(user_id_numeric)?;

        if let Some(username) = username.clone() {
            let username = username.to_lowercase_non_whitespace();
            let user = self.get_user(user_id)?;
            let existing_user = self.get_user(&username.clone().try_into()?);
            if existing_user.is_ok() && existing_user.unwrap().id != user.id {
                error!("User: {username} already exists.");
                return Err(IggyError::UserAlreadyExists);
            }
        }

        let mut user = self.get_user_mut(user_id)?;
        if let Some(username) = username {
            user.username = username;
        }

        if let Some(status) = status {
            user.status = status;
        }

        info!("Updated user: {} with ID: {}.", user.username, user.id);
        Ok(())
    }

    pub async fn update_permissions(
        &self,
        user_id_numeric: u32,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        {
            self.permissioner
                .borrow()
                .update_permissions(user_id_numeric)?;
            let user = self.get_user(user_id)?;
            if user.is_root() {
                error!("Cannot change the root user permissions.");
                return Err(IggyError::CannotChangePermissions(user.id));
            }

            self.permissioner
                .borrow_mut()
                .update_permissions_for_user(user.id, permissions.clone());
        }

        {
            let mut user = self.get_user_mut(user_id)?;
            user.permissions = permissions;
            info!(
                "Updated permissions for user: {} with ID: {user_id}.",
                user.username
            );
        }

        Ok(())
    }

    pub async fn change_password(
        &self,
        user_id_numeric: u32,
        user_id: &Identifier,
        current_password: String,
        new_password: String,
    ) -> Result<(), IggyError> {
        {
            let user = self.get_user(user_id)?;
            if user.id != user_id_numeric {
                self.permissioner
                    .borrow()
                    .change_password(user_id_numeric)?;
            }
        }

        let mut user = self.get_user_mut(user_id)?;
        if !crypto::verify_password(current_password, &user.password) {
            error!(
                "Invalid current password for user: {} with ID: {user_id}.",
                user.username
            );
            return Err(IggyError::InvalidCredentials);
        }

        user.password = crypto::hash_password(new_password);
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
        let user = match self.get_user(&username.clone().try_into()?) {
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
        // TODO - maybe this can be solved better ?
        let active_sessions = self.active_sessions.borrow();
        let session = active_sessions
            .iter()
            .find(|s| s.client_id == client_id)
            .expect(format!("At this point session for {}, should exist.", client_id).as_str());
        if self.ensure_authenticated(client_id).is_err() {
            session.set_user_id(user.id);
            return Ok(user);
        }

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

        let user = self.get_user(&Identifier::numeric(user_id)?)?;
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
