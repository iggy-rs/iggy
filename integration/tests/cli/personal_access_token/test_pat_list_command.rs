use crate::cli::common::{
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, OutputFormat, TestHelpCmd, CLAP_INDENT,
    USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestPatListCmd {
    name: String,
    output: OutputFormat,
}

impl TestPatListCmd {
    fn new(name: String, output: OutputFormat) -> Self {
        Self { name, output }
    }

    fn to_args(&self) -> Vec<String> {
        self.output
            .to_args()
            .into_iter()
            .map(String::from)
            .collect()
    }
}

#[async_trait]
impl IggyCmdTestCase for TestPatListCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let pat = client
            .create_personal_access_token(&self.name, PersonalAccessTokenExpiry::NeverExpire)
            .await;
        assert!(pat.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("pat")
            .arg("list")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with(format!(
                "Executing list personal access tokens in {} mode",
                self.output
            )))
            .stdout(contains(self.name.clone()));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let delete = client.delete_personal_access_token(&self.name).await;
        assert!(delete.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestPatListCmd::new(
            String::from("name"),
            OutputFormat::Default,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestPatListCmd::new(
            String::from("client"),
            OutputFormat::List,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestPatListCmd::new(
            String::from("short"),
            OutputFormat::Table,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["pat", "list", "--help"],
            format!(
                r#"List all personal access tokens

Examples
 iggy pat list

{USAGE_PREFIX} pat list [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>
          List mode (table or list)
{CLAP_INDENT}
          [default: table]
          [possible values: table, list]

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
            vec!["pat", "list", "-h"],
            format!(
                r#"List all personal access tokens

{USAGE_PREFIX} pat list [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
