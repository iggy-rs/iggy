use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::models::permissions::Permissions;
use crate::models::user_status::UserStatus;
use crate::users::create_user::CreateUser;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct CreateUserCmd {
    create_user: CreateUser,
}

impl CreateUserCmd {
    pub fn new(
        username: String,
        password: String,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Self {
        Self {
            create_user: CreateUser {
                username,
                password,
                status,
                permissions,
            },
        }
    }
}

#[async_trait]
impl CliCommand for CreateUserCmd {
    fn explain(&self) -> String {
        format!(
            "create user with username: {} and password: {}",
            self.create_user.username, self.create_user.password
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_user(
                &self.create_user.username,
                &self.create_user.password,
                self.create_user.status,
                self.create_user.permissions.clone(),
            )
            .await
            .with_context(|| {
                format!(
                    "Problem creating user (username: {} and password: {})",
                    self.create_user.username, self.create_user.password
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "User with username: {} and password: {} created",
            self.create_user.username, self.create_user.password
        );

        Ok(())
    }
}
