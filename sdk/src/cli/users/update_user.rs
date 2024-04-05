use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::models::user_status::UserStatus;
use crate::users::update_user::UpdateUser;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

#[derive(Debug, Clone)]
pub enum UpdateUserType {
    Name(String),
    Status(UserStatus),
}

pub struct UpdateUserCmd {
    update_type: UpdateUserType,
    update_user: UpdateUser,
}

impl UpdateUserCmd {
    pub fn new(user_id: Identifier, update_type: UpdateUserType) -> Self {
        let (username, status) = match update_type.clone() {
            UpdateUserType::Name(username) => (Some(username), None),
            UpdateUserType::Status(status) => (None, Some(status)),
        };

        UpdateUserCmd {
            update_type,
            update_user: UpdateUser {
                user_id,
                username,
                status,
            },
        }
    }

    fn get_message(&self) -> String {
        match &self.update_type {
            UpdateUserType::Name(username) => format!("username: {}", username),
            UpdateUserType::Status(status) => format!("status: {}", status),
        }
    }
}

#[async_trait]
impl CliCommand for UpdateUserCmd {
    fn explain(&self) -> String {
        format!(
            "update user with ID: {} with {}",
            self.update_user.user_id,
            self.get_message()
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .update_user(
                &self.update_user.user_id,
                self.update_user.username.as_deref(),
                self.update_user.status,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem updating user with ID: {} with {}",
                    self.update_user.user_id,
                    self.get_message()
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "User with ID: {} updated with {}",
            self.update_user.user_id, self.get_message()
        );

        Ok(())
    }
}
