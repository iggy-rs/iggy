use crate::cmd::common::{
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, OutputFormat, TestHelpCmd, TestStreamId,
    CLAP_INDENT, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::{client::Client, identifier::Identifier};
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestTopicListCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    using_stream_id: TestStreamId,
    output: OutputFormat,
}

impl TestTopicListCmd {
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        using_stream_id: TestStreamId,
        output: OutputFormat,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            using_stream_id,
            output,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        };

        args.extend(self.output.to_args().into_iter().map(String::from));

        args
    }
}

#[async_trait]
impl IggyCmdTestCase for TestTopicListCmd {
    async fn prepare_server_state(&self, client: &dyn Client) {
        let stream = client
            .create_stream(&CreateStream {
                stream_id: self.stream_id,
                name: self.stream_name.clone(),
            })
            .await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(&CreateTopic {
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
                topic_id: self.topic_id,
                partitions_count: 1,
                name: self.topic_name.clone(),
                message_expiry: None,
            })
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("topic")
            .arg("list")
            .args(self.to_args())
            .with_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = match self.using_stream_id {
            TestStreamId::Numeric => format!("{}", self.stream_id),
            TestStreamId::Named => self.stream_name.clone(),
        };

        command_state
            .success()
            .stdout(starts_with(format!(
                "Executing list topics from stream with ID: {} in {} mode",
                stream_id, self.output
            )))
            .stdout(contains(self.topic_name.clone()));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic = client
            .delete_topic(&DeleteTopic {
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
                topic_id: Identifier::numeric(self.topic_id).unwrap(),
            })
            .await;
        assert!(topic.is_ok());

        let stream = client
            .delete_stream(&DeleteStream {
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
            })
            .await;
        assert!(stream.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestTopicListCmd::new(
            1,
            String::from("main"),
            1,
            String::from("sync"),
            TestStreamId::Numeric,
            OutputFormat::Default,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicListCmd::new(
            2,
            String::from("customer"),
            3,
            String::from("topic"),
            TestStreamId::Named,
            OutputFormat::List,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicListCmd::new(
            3,
            String::from("production"),
            1,
            String::from("data"),
            TestStreamId::Numeric,
            OutputFormat::Table,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "list", "--help"],
            format!(
                r#"List all topics in given stream ID

Stream ID can be specified as a stream name or ID

Examples
 iggy topic list 1
 iggy topic list prod

{USAGE_PREFIX} topic list [OPTIONS] <STREAM_ID>

Arguments:
  <STREAM_ID>
          Stream ID to list topics
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

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
            vec!["topic", "list", "-h"],
            format!(
                r#"List all topics in given stream ID

{USAGE_PREFIX} topic list [OPTIONS] <STREAM_ID>

Arguments:
  <STREAM_ID>  Stream ID to list topics

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
