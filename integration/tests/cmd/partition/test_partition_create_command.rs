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
use iggy::topics::get_topic::GetTopic;
use iggy::{client::Client, identifier::Identifier};
use predicates::str::diff;
use serial_test::parallel;

struct TestPartitionCreateCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    partitions_count: u32,
    new_partitions: u32,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
}

impl TestPartitionCreateCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        partitions_count: u32,
        new_partitions: u32,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            partitions_count,
            new_partitions,
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

        command.push(format!("{}", self.new_partitions));

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestPartitionCreateCmd {
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
                partitions_count: self.partitions_count,
                name: self.topic_name.clone(),
                message_expiry: None,
            })
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("partition")
            .arg("create")
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

        let mut partitions = String::from("partition");
        if self.new_partitions > 1 {
            partitions.push('s');
        };

        let message = format!("Executing create {} {partitions} for topic with ID: {} and stream with ID: {}\nCreated {} {partitions} for topic with ID: {} and stream with ID: {}\n",
            self.new_partitions, topic_id, stream_id, self.new_partitions, topic_id, stream_id);

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic = client
            .get_topic(&GetTopic {
                topic_id: Identifier::numeric(self.topic_id).unwrap(),
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
            })
            .await;
        assert!(topic.is_ok());
        let topic_details = topic.unwrap();
        assert_eq!(topic_details.name, self.topic_name);
        assert_eq!(topic_details.id, self.topic_id);
        assert_eq!(
            topic_details.partitions_count,
            self.partitions_count + self.new_partitions
        );
        assert_eq!(topic_details.messages_count, 0);

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
        .execute_test(TestPartitionCreateCmd::new(
            1,
            String::from("main"),
            1,
            String::from("sync"),
            1,
            1,
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestPartitionCreateCmd::new(
            2,
            String::from("stream"),
            3,
            String::from("topic"),
            3,
            2,
            TestStreamId::Named,
            TestTopicId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestPartitionCreateCmd::new(
            4,
            String::from("development"),
            1,
            String::from("probe"),
            0,
            4,
            TestStreamId::Numeric,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestPartitionCreateCmd::new(
            2,
            String::from("production"),
            5,
            String::from("test"),
            4,
            1,
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
            vec!["partition", "create", "--help"],
            format!(
                r#"Create partitions for the specified topic ID
and stream ID based on the given count.

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples
 iggy partition create 1 1 10
 iggy partition create prod 2 2
 iggy partition create test sensor 2
 iggy partition create 1 sensor 16

{USAGE_PREFIX} partition create <STREAM_ID> <TOPIC_ID> <PARTITIONS_COUNT>

Arguments:
  <STREAM_ID>
          Stream ID to create partitions
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to create partitions
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <PARTITIONS_COUNT>
          Partitions count to be created

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
            vec!["partition", "create", "-h"],
            format!(
                r#"Create partitions for the specified topic ID
and stream ID based on the given count.

{USAGE_PREFIX} partition create <STREAM_ID> <TOPIC_ID> <PARTITIONS_COUNT>

Arguments:
  <STREAM_ID>         Stream ID to create partitions
  <TOPIC_ID>          Topic ID to create partitions
  <PARTITIONS_COUNT>  Partitions count to be created

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
