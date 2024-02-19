use crate::cli::system::session::ServerSession;
use crate::cli::utils::login_session_expiry::LoginSessionExpiry;
use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use crate::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

const DEFAULT_LOGIN_SESSION_TIMEOUT: u32 = 15 * 60;

pub struct LoginCmd {
    server_session: ServerSession,
    login_session_expiry: Option<LoginSessionExpiry>,
}

impl LoginCmd {
    pub fn new(server_address: String, login_session_expiry: Option<LoginSessionExpiry>) -> Self {
        Self {
            server_session: ServerSession::new(server_address),
            login_session_expiry,
        }
    }
}

#[async_trait]
impl CliCommand for LoginCmd {
    fn explain(&self) -> String {
        "login command".to_owned()
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        if self.server_session.is_active() {
            event!(target: PRINT_TARGET, Level::INFO, "Already logged into Iggy server {}", self.server_session.get_server_address());
            return Ok(());
        }

        let tokens = client
            .get_personal_access_tokens(&GetPersonalAccessTokens {})
            .await
            .with_context(|| {
                format!(
                    "Problem getting personal access tokens from server: {}",
                    self.server_session.get_server_address()
                )
            })?;

        if tokens
            .iter()
            .any(|pat| pat.name == self.server_session.get_token_name())
        {
            event!(target: PRINT_TARGET, Level::INFO, "Already logged into Iggy server {}", self.server_session.get_server_address());
            return Ok(());
        }

        let token = client
            .create_personal_access_token(&CreatePersonalAccessToken {
                name: self.server_session.get_token_name(),
                expiry: match &self.login_session_expiry {
                    None => Some(DEFAULT_LOGIN_SESSION_TIMEOUT),
                    Some(value) => value.into(),
                },
            })
            .await
            .with_context(|| {
                format!(
                    "Problem creating personal access token with name: {}",
                    self.server_session.get_token_name()
                )
            })?;

        self.server_session.store(&token.token)?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Successfully logged into Iggy server {}",
            self.server_session.get_server_address(),
        );

        Ok(())
    }
}
