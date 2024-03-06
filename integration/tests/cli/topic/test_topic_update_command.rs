use crate::cli::common::{
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
use iggy::utils::max_topic_size::MaxTopicSize;
use iggy::utils::message_expiry::MessageExpiry;
use iggy::{client::Client, identifier::Identifier};
use predicates::str::diff;
use serial_test::parallel;

struct TestTopicUpdateCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    message_expiry: Option<Vec<MessageExpiry>>,
    max_topic_size: Option<MaxTopicSize>,
    replication_factor: u8,
    topic_new_name: String,
    topic_new_message_expiry: Option<Vec<MessageExpiry>>,
    topic_new_max_size: Option<MaxTopicSize>,
    topic_new_replication_factor: u8,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
}

impl TestTopicUpdateCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        message_expiry: Option<Vec<MessageExpiry>>,
        max_topic_size: Option<MaxTopicSize>,
        replication_factor: u8,
        topic_new_name: String,
        topic_new_message_expiry: Option<Vec<MessageExpiry>>,
        topic_new_max_size: Option<MaxTopicSize>,
        topic_new_replication_factor: u8,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            message_expiry,
            max_topic_size,
            replication_factor,
            topic_new_name,
            topic_new_message_expiry,
            topic_new_max_size,
            topic_new_replication_factor,
            using_stream_id,
            using_topic_id,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        };

        args.push(match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        });

        args.push(self.topic_new_name.clone());

        if let Some(max_size_bytes) = &self.topic_new_max_size {
            args.push(format!("--max-size-bytes={}", max_size_bytes));
        }

        if self.topic_new_replication_factor != 1 {
            args.push(format!(
                "--replication-factor={}",
                self.topic_new_replication_factor
            ));
        }

        if let Some(message_expiry) = self.topic_new_message_expiry.clone() {
            message_expiry.iter().for_each(|expiry| {
                args.push(expiry.to_string());
            });
        }

        args
    }
}

#[async_trait]
impl IggyCmdTestCase for TestTopicUpdateCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&CreateStream {
                stream_id: Some(self.stream_id),
                name: self.stream_name.clone(),
            })
            .await;
        assert!(stream.is_ok());

        let message_expiry = if let Some(message_expiry) = self.message_expiry.clone() {
            message_expiry.into()
        } else {
            MessageExpiry::Unlimited
        };
        let max_topic_size = if let Some(max_topic_size) = self.max_topic_size {
            max_topic_size
        } else {
            MaxTopicSize::Unlimited
        };

        let topic = client
            .create_topic(&CreateTopic {
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
                topic_id: Some(self.topic_id),
                partitions_count: 1,
                name: self.topic_name.clone(),
                message_expiry,
                max_topic_size,
                replication_factor: self.replication_factor,
            })
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("topic")
            .arg("update")
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

        let message_expiry = match self.topic_new_message_expiry.clone() {
            Some(value) => value.into(),
            None => MessageExpiry::Unlimited,
        };

        let max_topic_size = self.topic_new_max_size.unwrap_or(MaxTopicSize::Unlimited);

        let replication_factor = self.replication_factor;
        let new_topic_name = &self.topic_new_name;

        let expected_message = format!("Executing update topic with ID: {topic_id}, name: {new_topic_name}, \
                                message expiry: {message_expiry}, max topic size: {max_topic_size}, \
                                replication factor: {replication_factor}, in stream with ID: {stream_id}\n\
                                Topic with ID: {topic_id} updated name: {new_topic_name}, updated message expiry: {message_expiry} \
                                in stream with ID: {stream_id}\n");

        command_state.success().stdout(diff(expected_message));
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
        assert_eq!(topic_details.name, self.topic_new_name);
        assert_eq!(topic_details.id, self.topic_id);
        assert_eq!(topic_details.messages_count, 0);

        if let Some(message_expiry) = self.topic_new_message_expiry.clone() {
            assert_eq!(topic_details.message_expiry, message_expiry.into());
        }

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
    use std::str::FromStr;
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestTopicUpdateCmd::new(
            1,
            String::from("main"),
            1,
            String::from("sync"),
            None,
            None,
            1,
            String::from("new_name"),
            None,
            None,
            1,
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicUpdateCmd::new(
            2,
            String::from("production"),
            3,
            String::from("topic"),
            None,
            None,
            1,
            String::from("testing"),
            None,
            None,
            1,
            TestStreamId::Named,
            TestTopicId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicUpdateCmd::new(
            3,
            String::from("testing"),
            5,
            String::from("development"),
            None,
            None,
            1,
            String::from("development"),
            None,
            None,
            1,
            TestStreamId::Numeric,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicUpdateCmd::new(
            2,
            String::from("other"),
            2,
            String::from("probe"),
            None,
            None,
            1,
            String::from("development"),
            Some(vec![
                MessageExpiry::from_str("1day").unwrap(),
                MessageExpiry::from_str("1h").unwrap(),
                MessageExpiry::from_str("1m").unwrap(),
                MessageExpiry::from_str("1s").unwrap(),
            ]),
            None,
            1,
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicUpdateCmd::new(
            3,
            String::from("stream"),
            1,
            String::from("testing"),
            Some(vec![MessageExpiry::from_str("1s").unwrap()]),
            None,
            1,
            String::from("testing"),
            Some(vec![MessageExpiry::from_str("1m 6s").unwrap()]),
            None,
            1,
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicUpdateCmd::new(
            4,
            String::from("testing"),
            2,
            String::from("testing"),
            Some(vec![
                MessageExpiry::from_str("1s").unwrap(),
                MessageExpiry::from_str("1m").unwrap(),
                MessageExpiry::from_str("1h").unwrap(),
            ]),
            None,
            1,
            String::from("testing"),
            None,
            None,
            1,
            TestStreamId::Numeric,
            TestTopicId::Named,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "update", "--help"],
            format!(
                r#"Update topic name an message expiry time for given topic ID in given stream ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples
 iggy update 1 1 sensor3
 iggy update prod sensor3 old-sensor
 iggy update test debugs ready 15days
 iggy update 1 1 new-name
 iggy update 1 2 new-name 1day 1hour 1min 1sec

{USAGE_PREFIX} topic update [OPTIONS] <STREAM_ID> <TOPIC_ID> <NAME> [MESSAGE_EXPIRY]...

Arguments:
  <STREAM_ID>
          Stream ID to update topic
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to update
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <NAME>
          New name for the topic

  [MESSAGE_EXPIRY]...
          New message expiry time in human readable format like 15days 2min 2s
{CLAP_INDENT}
          ("unlimited" or skipping parameter causes removal of expiry parameter in topic)

Options:
  -m, --max-topic-size <MAX_TOPIC_SIZE>
          New max topic size
{CLAP_INDENT}
          ("unlimited" or skipping parameter causes removal of max topic size parameter in topic)
          Can't be lower than segment size in the config.
{CLAP_INDENT}
          [default: unlimited]

  -r, --replication-factor <REPLICATION_FACTOR>
          New replication factor for the topic
{CLAP_INDENT}
          [default: 1]

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
            vec!["topic", "update", "-h"],
            format!(
                r#"Update topic name an message expiry time for given topic ID in given stream ID

{USAGE_PREFIX} topic update [OPTIONS] <STREAM_ID> <TOPIC_ID> <NAME> [MESSAGE_EXPIRY]...

Arguments:
  <STREAM_ID>          Stream ID to update topic
  <TOPIC_ID>           Topic ID to update
  <NAME>               New name for the topic
  [MESSAGE_EXPIRY]...  New message expiry time in human readable format like 15days 2min 2s

Options:
  -m, --max-topic-size <MAX_TOPIC_SIZE>          New max topic size [default: unlimited]
  -r, --replication-factor <REPLICATION_FACTOR>  New replication factor for the topic [default: 1]
  -h, --help                                     Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
