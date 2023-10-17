use crate::cmd::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::streams::get_stream::GetStream;
use iggy::{client::Client, identifier::Identifier};
use predicates::str::diff;
use serial_test::parallel;

struct TestStreamCreateCmd {
    stream_id: u32,
    name: String,
}

impl TestStreamCreateCmd {
    fn new(stream_id: u32, name: String) -> Self {
        Self { stream_id, name }
    }

    fn to_args(&self) -> Vec<String> {
        vec![format!("{}", self.stream_id), self.name.clone()]
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamCreateCmd {
    async fn prepare_server_state(&self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stream")
            .arg("create")
            .args(self.to_args())
            .with_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(diff(format!("Executing create stream with ID: {} and name: {}\nStream with ID: {} and name: {} created\n", self.stream_id, self.name, self.stream_id, self.name)));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let stream = client
            .get_stream(&GetStream {
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
            })
            .await;
        assert!(stream.is_ok());
        let stream = stream.unwrap();
        assert_eq!(stream.name, self.name);
        assert_eq!(stream.id, self.stream_id);
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let stream_id = 123;
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamCreateCmd::new(stream_id, String::from("main")))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["stream", "create", "--help"],
            format!(
                r#"Create stream with given ID and name

Examples:
 iggy stream create 1 prod
 iggy stream create 2 test

{USAGE_PREFIX} stream create <STREAM_ID> <NAME>

Arguments:
  <STREAM_ID>
          Stream ID to create topic

  <NAME>
          Name of the stream

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
            vec!["stream", "create", "-h"],
            format!(
                r#"Create stream with given ID and name

{USAGE_PREFIX} stream create <STREAM_ID> <NAME>

Arguments:
  <STREAM_ID>  Stream ID to create topic
  <NAME>       Name of the stream

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
