use crate::cmd::common::{
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId, TestTopicId,
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

struct TestTopicGetCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
}

impl TestTopicGetCmd {
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            using_stream_id,
            using_topic_id,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        };

        command.push(match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        });

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestTopicGetCmd {
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
            .arg("get")
            .args(self.to_args())
            .with_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = match self.using_stream_id {
            TestStreamId::Numeric => format!("{}", self.stream_id),
            TestStreamId::Named => self.stream_name.clone(),
        };

        let topic_id = match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        };

        let start_message = format!(
            "Executing get topic with ID: {} from stream with ID: {}",
            topic_id, stream_id
        );

        command_state
            .success()
            .stdout(starts_with(start_message))
            .stdout(contains(format!("Topic id            | {}", self.topic_id)))
            .stdout(contains(format!(
                "Topic name          | {}",
                self.topic_name
            )))
            .stdout(contains("Topic size          | 0"))
            .stdout(contains("Message expiry      | None"))
            .stdout(contains("Topic message count | 0"))
            .stdout(contains("Partitions count    | 1"));
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
        .execute_test(TestTopicGetCmd::new(
            1,
            String::from("main"),
            1,
            String::from("sync"),
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            2,
            String::from("customer"),
            4,
            String::from("probe"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            2,
            String::from("development"),
            3,
            String::from("testing"),
            TestStreamId::Numeric,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            7,
            String::from("other"),
            2,
            String::from("topic"),
            TestStreamId::Named,
            TestTopicId::Numeric,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "get", "--help"],
            format!(
                r#"Get topic detail for given topic ID and stream ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples
 iggy topic get 1 1
 iggy topic get prod 2
 iggy topic get test debugs
 iggy topic get 2 debugs

{USAGE_PREFIX} topic get <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>
          Stream ID to get topic
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to get
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

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
            vec!["topic", "get", "-h"],
            format!(
                r#"Get topic detail for given topic ID and stream ID

{USAGE_PREFIX} topic get <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>  Stream ID to get topic
  <TOPIC_ID>   Topic ID to get

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
