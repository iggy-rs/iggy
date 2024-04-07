use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use predicates::str::{contains, starts_with};
use serial_test::parallel;

#[derive(Debug, Default)]
enum UseCredentials {
    #[default]
    CliOptions,
    StdinInput,
}

#[derive(Debug, Default)]
struct TestLoginOptions {
    use_credentials: UseCredentials,
}

impl TestLoginOptions {
    fn new(use_credentials: UseCredentials) -> Self {
        Self { use_credentials }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestLoginOptions {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        match self.use_credentials {
            UseCredentials::CliOptions => IggyCmdCommand::new().with_cli_credentials().arg("me"),
            UseCredentials::StdinInput => IggyCmdCommand::new()
                .opt("--username")
                .opt(DEFAULT_ROOT_USERNAME)
                .arg("me"),
        }
    }

    fn provide_stdin_input(&self) -> Option<Vec<String>> {
        match self.use_credentials {
            UseCredentials::StdinInput => Some(vec![DEFAULT_ROOT_PASSWORD.to_string()]),
            _ => None,
        }
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with("Executing me command\n"))
            .stdout(contains(String::from("Transport | TCP")));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestLoginOptions::new(UseCredentials::CliOptions))
        .await;
    iggy_cmd_test
        .execute_test(TestLoginOptions::new(UseCredentials::StdinInput))
        .await;
}
