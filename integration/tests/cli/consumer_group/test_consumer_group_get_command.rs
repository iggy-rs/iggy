use crate::cli::common::{
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestConsumerGroupId, TestHelpCmd, TestStreamId,
    TestTopicId, CLAP_INDENT, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::{client::Client, identifier::Identifier};
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestConsumerGroupGetCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    consumer_group_id: u32,
    consumer_group_name: String,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
    using_consumer_group_id: TestConsumerGroupId,
}

impl TestConsumerGroupGetCmd {
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
        using_consumer_group_id: TestConsumerGroupId,
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
            using_consumer_group_id,
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

        command.push(match self.using_consumer_group_id {
            TestConsumerGroupId::Numeric => format!("{}", self.consumer_group_id),
            TestConsumerGroupId::Named => self.consumer_group_name.clone(),
        });

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerGroupGetCmd {
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

        let consumer_group = client
            .create_consumer_group(&CreateConsumerGroup {
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
                topic_id: Identifier::numeric(self.topic_id).unwrap(),
                consumer_group_id: self.consumer_group_id,
                name: self.consumer_group_name.clone(),
            })
            .await;
        assert!(consumer_group.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("consumer-group")
            .arg("get")
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

        let consumer_group_id = match self.using_consumer_group_id {
            TestConsumerGroupId::Numeric => format!("{}", self.consumer_group_id),
            TestConsumerGroupId::Named => self.consumer_group_name.clone(),
        };

        let start_message = format!(
            "Executing get consumer group with ID: {} for topic with ID: {} and stream with ID: {}",
            consumer_group_id, topic_id, stream_id
        );

        command_state
            .success()
            .stdout(starts_with(start_message))
            .stdout(contains(format!(
                "Consumer group id   | {}",
                self.consumer_group_id
            )))
            .stdout(contains(format!(
                "Consumer group name | {}",
                self.consumer_group_name
            )));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let consumer_group = client
            .delete_consumer_group(&DeleteConsumerGroup {
                topic_id: Identifier::numeric(self.topic_id).unwrap(),
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
                consumer_group_id: Identifier::numeric(self.consumer_group_id).unwrap(),
            })
            .await;
        assert!(consumer_group.is_ok());

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

    let test_parameters = vec![
        (
            TestStreamId::Numeric,
            TestTopicId::Numeric,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Numeric,
            TestConsumerGroupId::Named,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Named,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Named,
            TestConsumerGroupId::Named,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Numeric,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Numeric,
            TestConsumerGroupId::Named,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Named,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Named,
            TestConsumerGroupId::Named,
        ),
    ];

    iggy_cmd_test.setup().await;
    for (using_stream_id, using_topic_id, using_consumer_group_id) in test_parameters {
        iggy_cmd_test
            .execute_test(TestConsumerGroupGetCmd::new(
                1,
                String::from("stream"),
                2,
                String::from("topic"),
                3,
                String::from("consumer-group"),
                using_stream_id,
                using_topic_id,
                using_consumer_group_id,
            ))
            .await;
    }
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["consumer-group", "get", "--help"],
            format!(
                r#"Get details of a single consumer group with given ID for given stream ID and topic ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID
Consumer group ID can be specified as a consumer group name or ID

Examples:
 iggy consumer-group get 1 2 3
 iggy consumer-group get stream 2 3
 iggy consumer-group get 1 topic 3
 iggy consumer-group get 1 2 group
 iggy consumer-group get stream topic 3
 iggy consumer-group get 1 topic group
 iggy consumer-group get stream 2 group
 iggy consumer-group get stream topic group

{USAGE_PREFIX} consumer-group get <STREAM_ID> <TOPIC_ID> <CONSUMER_GROUP_ID>

Arguments:
  <STREAM_ID>
          Stream ID to get consumer group
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to get consumer group
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <CONSUMER_GROUP_ID>
          Consumer group ID to get
{CLAP_INDENT}
          Consumer group ID can be specified as a consumer group name or ID

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
            vec!["consumer-group", "get", "-h"],
            format!(
                r#"Get details of a single consumer group with given ID for given stream ID and topic ID

{USAGE_PREFIX} consumer-group get <STREAM_ID> <TOPIC_ID> <CONSUMER_GROUP_ID>

Arguments:
  <STREAM_ID>          Stream ID to get consumer group
  <TOPIC_ID>           Topic ID to get consumer group
  <CONSUMER_GROUP_ID>  Consumer group ID to get

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
