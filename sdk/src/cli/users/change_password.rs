use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use anyhow::Context;
use async_trait::async_trait;
use passterm::{isatty, prompt_password_stdin, prompt_password_tty, Stream};
use tracing::{event, Level};

pub struct ChangePasswordCmd {
    user_id: Identifier,
    current_password: Option<String>,
    new_password: Option<String>,
}

impl ChangePasswordCmd {
    pub fn new(
        user_id: Identifier,
        current_password: Option<String>,
        new_password: Option<String>,
    ) -> Self {
        Self {
            user_id,
            current_password,
            new_password,
        }
    }

    fn use_tracing(&self) -> bool {
        self.current_password.is_some() || self.new_password.is_some()
    }
}

#[async_trait]
impl CliCommand for ChangePasswordCmd {
    fn explain(&self) -> String {
        format!("change password for user with ID: {}", self.user_id,)
    }

    fn use_tracing(&self) -> bool {
        self.use_tracing()
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let current_password = match &self.current_password {
            Some(password) => password.clone(),
            None => {
                if isatty(Stream::Stdin) {
                    prompt_password_tty(Some("Current password: "))?
                } else {
                    prompt_password_stdin(None, Stream::Stdout)?
                }
            }
        };

        let new_password = match &self.new_password {
            Some(password) => password.clone(),
            None => {
                if isatty(Stream::Stdin) {
                    prompt_password_tty(Some("New password: "))?
                } else {
                    prompt_password_stdin(None, Stream::Stdout)?
                }
            }
        };

        client
            .change_password(&self.user_id, &current_password, &new_password)
            .await
            .with_context(|| {
                format!(
                    "Problem changing password for user with ID: {}",
                    self.user_id,
                )
            })?;

        if self.use_tracing() {
            event!(target: PRINT_TARGET, Level::INFO, "Password for user with ID: {} changed", self.user_id);
        } else {
            println!("Password for user with ID: {} changed", self.user_id);
        }

        Ok(())
    }
}
