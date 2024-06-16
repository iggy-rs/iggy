use super::shard::IggyShard;
use crate::streaming::storage::{Storage, UserStorage};
use crate::streaming::users::user::User;
use iggy::{
    error::IggyError,
    users::defaults::{
        DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH,
        MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
    },
};
use std::{
    env,
    sync::atomic::{AtomicU32, Ordering},
};
use tracing::info;

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
        self.permissioner.init(users);
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

        User::root(&username, &password)
    }
}
