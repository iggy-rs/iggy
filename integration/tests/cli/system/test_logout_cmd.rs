use crate::cli::common::{IggyCmdCommand, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::cli::system::session::ServerSession;
use iggy::client::Client;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use predicates::str::diff;

#[derive(Debug)]
pub(super) struct TestLogoutCmd {
    server_address: String,
}

impl TestLogoutCmd {
    pub(super) fn new(server_address: String) -> Self {
        Self { server_address }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestLogoutCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let login_session = ServerSession::new(self.server_address.clone());
        assert!(!login_session.is_active());

        let token = client
            .create_personal_access_token(&CreatePersonalAccessToken {
                name: login_session.get_token_name(),
                expiry: None,
            })
            .await;
        assert!(token.is_ok());

        let token = token.unwrap();
        login_session.store(&token.token).unwrap();
        assert!(login_session.is_active());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new().arg("logout")
    }

    fn verify_command(&self, command_state: Assert) {
        command_state.success().stdout(diff(format!(
            "Executing logout command\nSuccessfully logged out from Iggy server {}\n",
            self.server_address
        )));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let login_session = ServerSession::new(self.server_address.clone());
        assert!(!login_session.is_active());

        let pats = client
            .get_personal_access_tokens(&GetPersonalAccessTokens {})
            .await
            .unwrap();
        assert_eq!(pats.len(), 0);
    }
}
