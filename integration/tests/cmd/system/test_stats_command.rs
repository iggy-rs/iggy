use crate::cmd::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::streams::create_stream::CreateStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::{client::Client, identifier::Identifier};
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestStatsCmd {}

#[async_trait]
impl IggyCmdTestCase for TestStatsCmd {
    async fn prepare_server_state(&self, client: &dyn Client) {
        let stream_id = Identifier::from_str_value("logs").unwrap();
        let stream = client
            .create_stream(&CreateStream {
                stream_id: 1,
                name: stream_id.as_string(),
            })
            .await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(&CreateTopic {
                topic_id: 1,
                stream_id,
                partitions_count: 5,
                message_expiry: None,
                name: String::from("topic"),
            })
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new().arg("stats").with_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with("Executing stats command\n"))
            .stdout(contains("Streams Count            | 1"))
            .stdout(contains("Topics Count             | 1"))
            .stdout(contains("Partitions Count         | 5"))
            .stdout(contains("Segments Count           | 5"))
            .stdout(contains("Message Count            | 0"))
            .stdout(contains("Clients Count            | 2")) // 2 clients are connected during test
            .stdout(contains("Consumer Groups Count    | 0"));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test.execute_test(TestStatsCmd {}).await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["stats", "--help"],
            format!(
                r#"get iggy server statistics

Collect basic Iggy server statistics like number of streams, topics, partitions, etc. Server OS name, version, etc. are also collected.

{USAGE_PREFIX} stats

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
            vec!["stats", "-h"],
            format!(
                r#"get iggy server statistics

{USAGE_PREFIX} stats

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
