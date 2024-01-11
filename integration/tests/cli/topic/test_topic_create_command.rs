use crate::cli::common::{
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId, CLAP_INDENT,
    USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use iggy::cli::utils::message_expiry::MessageExpiry;
use iggy::streams::create_stream::CreateStream;
use iggy::topics::get_topic::GetTopic;
use iggy::utils::byte_size::IggyByteSize;
use iggy::{client::Client, identifier::Identifier};
use predicates::str::diff;
use serial_test::parallel;
use std::time::Duration;

struct TestTopicCreateCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    partitions_count: u32,
    message_expiry: Option<Vec<String>>,
    max_topic_size: Option<IggyByteSize>,
    replication_factor: u8,
    using_identifier: TestStreamId,
}

impl TestTopicCreateCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        partitions_count: u32,
        message_expiry: Option<Vec<String>>,
        max_topic_size: Option<IggyByteSize>,
        replication_factor: u8,
        using_identifier: TestStreamId,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            partitions_count,
            message_expiry,
            max_topic_size,
            replication_factor,
            using_identifier,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = match self.using_identifier {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        };

        command.push(format!("{}", self.topic_id));
        command.push(format!("{}", self.partitions_count));
        command.push(self.topic_name.clone());
        command.extend(self.message_expiry.clone().unwrap_or_default());

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestTopicCreateCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&CreateStream {
                stream_id: self.stream_id,
                name: self.stream_name.clone(),
            })
            .await;
        assert!(stream.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("topic")
            .arg("create")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = match self.using_identifier {
            TestStreamId::Numeric => format!("{}", self.stream_id),
            TestStreamId::Named => self.stream_name.clone(),
        };
        let partitions_count = self.partitions_count;
        let topic_id = &self.topic_id;
        let topic_name = &self.topic_name;
        let message_expiry = (match &self.message_expiry {
            Some(value) => value.join(" "),
            None => MessageExpiry::NeverExpire.to_string(),
        })
        .to_string();

        let max_topic_size = (match &self.max_topic_size {
            Some(value) => value.as_human_string_with_zero_as_unlimited(),
            None => IggyByteSize::default().as_human_string_with_zero_as_unlimited(),
        })
        .to_string();

        let replication_factor = self.replication_factor;

        let message = format!(
            "Executing create topic with ID: {topic_id}, name: {topic_name}, message expiry: {message_expiry}, \
            max topic size: {max_topic_size}, replication factor: {replication_factor} in stream with ID: {stream_id}\n\
            Topic with ID: {topic_id}, name: {topic_name}, partitions count: {partitions_count}, message expiry: {message_expiry}, \
            max topic size: {max_topic_size}, replication factor: {replication_factor} created in stream with ID: {stream_id}\n",
        );

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
        assert_eq!(topic_details.partitions_count, self.partitions_count);
        assert_eq!(topic_details.messages_count, 0);

        if self.message_expiry.is_some() {
            let duration: Duration = *self
                .message_expiry
                .clone()
                .unwrap()
                .join(" ")
                .parse::<HumanDuration>()
                .unwrap();
            assert_eq!(
                topic_details.message_expiry,
                Some(duration.as_secs() as u32)
            );
        }
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestTopicCreateCmd::new(
            1,
            String::from("main"),
            1,
            String::from("sync"),
            1,
            None,
            None,
            1,
            TestStreamId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicCreateCmd::new(
            2,
            String::from("testing"),
            2,
            String::from("topic"),
            5,
            None,
            None,
            1,
            TestStreamId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicCreateCmd::new(
            3,
            String::from("prod"),
            1,
            String::from("named"),
            1,
            Some(vec![String::from("3days"), String::from("5s")]),
            None,
            1,
            TestStreamId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicCreateCmd::new(
            4,
            String::from("big"),
            1,
            String::from("probe"),
            2,
            Some(vec![
                String::from("1day"),
                String::from("1h"),
                String::from("1m"),
                String::from("1s"),
            ]),
            None,
            1,
            TestStreamId::Numeric,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "create", "--help"],
            format!(
                r#"Create topic with given ID, name, number of partitions
and expiry time for given stream ID

Stream ID can be specified as a stream name or ID

Examples
 iggy topic create 1 1 2 sensor1 15days
 iggy topic create prod 2 2 sensor2
 iggy topic create test 3 2 debugs 1day 1hour 1min 1sec

{USAGE_PREFIX} topic create [OPTIONS] <STREAM_ID> <TOPIC_ID> <PARTITIONS_COUNT> <NAME> [MESSAGE_EXPIRY]...

Arguments:
  <STREAM_ID>
          Stream ID to create topic
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to create

  <PARTITIONS_COUNT>
          Number of partitions inside the topic

  <NAME>
          Name of the topic

  [MESSAGE_EXPIRY]...
          Message expiry time in human readable format like 15days 2min 2s
{CLAP_INDENT}
          ("unlimited" or skipping parameter disables message expiry functionality in topic)

Options:
  -m, --max-topic-size <MAX_TOPIC_SIZE>
          Max topic size
{CLAP_INDENT}
          ("unlimited" or skipping parameter disables max topic size functionality in topic)
          Can't be lower than segment size in the config.
{CLAP_INDENT}
          [default: unlimited]

  -r, --replication-factor <REPLICATION_FACTOR>
          Replication factor for the topic
{CLAP_INDENT}
          [default: 1]

  -h, --help
          Print help (see a summary with '-h')
"#,
        )))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_short_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "create", "-h"],
            format!(
                r#"Create topic with given ID, name, number of partitions
and expiry time for given stream ID

{USAGE_PREFIX} topic create [OPTIONS] <STREAM_ID> <TOPIC_ID> <PARTITIONS_COUNT> <NAME> [MESSAGE_EXPIRY]...

Arguments:
  <STREAM_ID>          Stream ID to create topic
  <TOPIC_ID>           Topic ID to create
  <PARTITIONS_COUNT>   Number of partitions inside the topic
  <NAME>               Name of the topic
  [MESSAGE_EXPIRY]...  Message expiry time in human readable format like 15days 2min 2s

Options:
  -m, --max-topic-size <MAX_TOPIC_SIZE>          Max topic size [default: unlimited]
  -r, --replication-factor <REPLICATION_FACTOR>  Replication factor for the topic [default: 1]
  -h, --help                                     Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
