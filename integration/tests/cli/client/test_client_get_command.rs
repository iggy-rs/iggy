use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestClientGetCmd {
    client_id: Option<u32>,
}

impl TestClientGetCmd {
    fn new() -> Self {
        Self { client_id: None }
    }

    fn get_client_id(&self) -> String {
        match self.client_id {
            None => String::from(""),
            Some(client_id) => format!("{}", client_id),
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestClientGetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let client_info = client.get_me().await;
        assert!(client_info.is_ok());
        self.client_id = Some(client_info.unwrap().client_id);
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("client")
            .arg("get")
            .arg(self.get_client_id())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with(format!(
                "Executing get client with ID: {}\n",
                self.get_client_id()
            )))
            .stdout(contains(format!(
                "Client ID             | {}",
                self.get_client_id()
            )))
            .stdout(contains("User ID               | 1"));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test.execute_test(TestClientGetCmd::new()).await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["client", "get", "--help"],
            format!(
                r#"Get details of a single client with given ID

Client ID is unique numerical identifier not to be confused with the user.

Examples:
 iggy client get 42

{USAGE_PREFIX} client get <CLIENT_ID>

Arguments:
  <CLIENT_ID>
          Client ID to get

Options:
  -h, --help
          Print help (see a summary with '-h')
"#,
            ),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_short_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["client", "get", "-h"],
            format!(
                r#"Get details of a single client with given ID

{USAGE_PREFIX} client get <CLIENT_ID>

Arguments:
  <CLIENT_ID>  Client ID to get

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
