use crate::cli::common::{
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, OutputFormat, TestHelpCmd, CLAP_INDENT,
    USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestStreamListCmd {
    stream_id: u32,
    name: String,
    output: OutputFormat,
}

impl TestStreamListCmd {
    fn new(stream_id: u32, name: String, output: OutputFormat) -> Self {
        Self {
            stream_id,
            name,
            output,
        }
    }

    fn to_args(&self) -> Vec<&str> {
        self.output.to_args()
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamListCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.name, Some(self.stream_id)).await;
        assert!(stream.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stream")
            .arg("list")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with(format!(
                "Executing list streams in {} mode",
                self.output
            )))
            .stdout(contains(self.name.clone()));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamListCmd::new(
            1,
            String::from("prod"),
            OutputFormat::Default,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamListCmd::new(
            2,
            String::from("testing"),
            OutputFormat::List,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamListCmd::new(
            3,
            String::from("misc"),
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
            vec!["stream", "list", "--help"],
            format!(
                r#"List all streams

Examples:
 iggy stream list
 iggy stream list --list-mode table
 iggy stream list -l table

{USAGE_PREFIX} stream list [OPTIONS]

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
            vec!["stream", "list", "-h"],
            format!(
                r#"List all streams

{USAGE_PREFIX} stream list [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
