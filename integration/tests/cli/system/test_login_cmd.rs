use crate::cli::common::{IggyCmdCommand, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::cli::system::session::ServerSession;
use iggy::client::Client;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use iggy::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use predicates::str::diff;

#[derive(Debug)]
pub(super) struct TestLoginCmd {
    server_address: String,
}

impl TestLoginCmd {
    pub(super) fn new(server_address: String) -> Self {
        Self { server_address }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestLoginCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let login_session = ServerSession::new(self.server_address.clone());
        assert!(!login_session.is_active());

        let pats = client
            .get_personal_access_tokens(&GetPersonalAccessTokens {})
            .await
            .unwrap();
        assert_eq!(pats.len(), 0);
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new().with_env_credentials().arg("login")
    }

    fn verify_command(&self, command_state: Assert) {
        command_state.success().stdout(diff(format!(
            "Executing login command\nSuccessfully logged into Iggy server {}\n",
            self.server_address
        )));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let login_session = ServerSession::new(self.server_address.clone());
        assert!(login_session.is_active());

        let pats = client
            .get_personal_access_tokens(&GetPersonalAccessTokens {})
            .await
            .unwrap();
        assert_eq!(pats.len(), 1);
        assert_eq!(pats[0].name, login_session.get_token_name());

        let cleanup_session = login_session.delete();
        assert!(cleanup_session.is_ok());

        let token_cleanup = client
            .delete_personal_access_token(&DeletePersonalAccessToken {
                name: login_session.get_token_name(),
            })
            .await;
        assert!(token_cleanup.is_ok());
    }
}
