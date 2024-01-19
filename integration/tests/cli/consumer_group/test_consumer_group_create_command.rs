use crate::cli::common::{
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId, TestTopicId,
    CLAP_INDENT, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::{client::Client, identifier::Identifier};
use predicates::str::diff;
use serial_test::parallel;

struct TestConsumerGroupCreateCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    consumer_group_id: u32,
    consumer_group_name: String,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
}

impl TestConsumerGroupCreateCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        consumer_group_id: u32,
        consumer_group_name: String,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            consumer_group_id,
            consumer_group_name,
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

        command.push(format!("{}", self.consumer_group_id));
        command.push(self.consumer_group_name.clone());

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerGroupCreateCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&CreateStream {
                stream_id: Some(self.stream_id),
                name: self.stream_name.clone(),
            })
            .await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(&CreateTopic {
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
                topic_id: Some(self.topic_id),
                partitions_count: 0,
                name: self.topic_name.clone(),
                message_expiry: None,
                max_topic_size: None,
                replication_factor: 1,
            })
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("consumer-group")
            .arg("create")
            .args(self.to_args())
            .with_env_credentials()
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

        let message = format!("Executing create consumer group with ID: {}, name: {} for topic with ID: {} and stream with ID: {}\nConsumer group with ID: {}, name: {} created for topic with ID: {} and stream with ID: {}\n",
            self.consumer_group_id, self.consumer_group_name, topic_id, stream_id, self.consumer_group_id, self.consumer_group_name, topic_id, stream_id);

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let consumer_group = client
            .get_consumer_group(&GetConsumerGroup {
                topic_id: Identifier::numeric(self.topic_id).unwrap(),
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
                consumer_group_id: Identifier::numeric(self.consumer_group_id).unwrap(),
            })
            .await;
        assert!(consumer_group.is_ok());
        let consumer_group_details = consumer_group.unwrap();
        assert_eq!(consumer_group_details.name, self.consumer_group_name);
        assert_eq!(consumer_group_details.id, self.consumer_group_id);

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
        .execute_test(TestConsumerGroupCreateCmd::new(
            1,
            String::from("main"),
            1,
            String::from("sync"),
            1,
            String::from("group1"),
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestConsumerGroupCreateCmd::new(
            2,
            String::from("stream"),
            3,
            String::from("topic"),
            3,
            String::from("group3"),
            TestStreamId::Named,
            TestTopicId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestConsumerGroupCreateCmd::new(
            4,
            String::from("development"),
            1,
            String::from("probe"),
            7,
            String::from("group7"),
            TestStreamId::Numeric,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestConsumerGroupCreateCmd::new(
            2,
            String::from("production"),
            5,
            String::from("test"),
            4,
            String::from("group4"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["consumer-group", "create", "--help"],
            format!(
                r#"Create consumer group with given ID and name for given stream ID and topic ID.

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples:
 iggy consumer-group create 1 1 1 prod
 iggy consumer-group create stream 2 2 test
 iggy consumer-group create 2 topic 3 receiver
 iggy consumer-group create stream topic 4 group

{USAGE_PREFIX} consumer-group create <STREAM_ID> <TOPIC_ID> <CONSUMER_GROUP_ID> <NAME>

Arguments:
  <STREAM_ID>
          Stream ID to create consumer group
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to create consumer group
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <CONSUMER_GROUP_ID>
          Consumer group ID to create

  <NAME>
          Consumer group name to create

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
            vec!["consumer-group", "create", "-h"],
            format!(
                r#"Create consumer group with given ID and name for given stream ID and topic ID.

{USAGE_PREFIX} consumer-group create <STREAM_ID> <TOPIC_ID> <CONSUMER_GROUP_ID> <NAME>

Arguments:
  <STREAM_ID>          Stream ID to create consumer group
  <TOPIC_ID>           Topic ID to create consumer group
  <CONSUMER_GROUP_ID>  Consumer group ID to create
  <NAME>               Consumer group name to create

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
