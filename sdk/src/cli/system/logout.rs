use crate::cli::system::session::ServerSession;
use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct LogoutCmd {
    server_session: ServerSession,
}

impl LogoutCmd {
    pub fn new(server_address: String) -> Self {
        Self {
            server_session: ServerSession::new(server_address),
        }
    }
}

#[async_trait]
impl CliCommand for LogoutCmd {
    fn explain(&self) -> String {
        "logout command".to_owned()
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        if self.server_session.is_active() {
            client
                .delete_personal_access_token(&self.server_session.get_token_name())
                .await
                .with_context(|| {
                    format!(
                        "Problem deleting personal access token with name: {}",
                        self.server_session.get_token_name()
                    )
                })?;

            self.server_session.delete()?;
        }
        event!(target: PRINT_TARGET, Level::INFO, "Successfully logged out from Iggy server {}", self.server_session.get_server_address());

        Ok(())
    }
}
