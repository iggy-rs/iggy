use crate::cli::common::{
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId, TestTopicId,
    CLAP_INDENT, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::messages::poll_messages::{PollingKind, PollingStrategy};
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::{client::Client, identifier::Identifier};
use predicates::str::{contains, starts_with};
use serial_test::parallel;
use std::str::FromStr;

struct TestMessagePollCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    partitions_count: u32,
    messages: Vec<String>,
    partition_id: u32,
    message_count: usize,
    strategy: PollingStrategy,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
}

impl TestMessagePollCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        partitions_count: u32,
        messages: &[String],
        partition_id: u32,
        message_count: usize,
        strategy: PollingStrategy,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        assert!(partition_id <= partitions_count);
        assert!(partition_id > 0);
        assert!(message_count < messages.len());
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            partitions_count,
            messages: messages.to_owned(),
            partition_id,
            message_count,
            strategy,
            using_stream_id,
            using_topic_id,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = match self.strategy.kind {
            PollingKind::Offset => vec!["--offset".into(), format!("{}", self.strategy.value)],
            PollingKind::Timestamp => {
                todo!("Timestamp based message polling is not supported yet")
            }
            PollingKind::First => vec!["--first".into()],
            PollingKind::Next => vec!["--next".into()],
            PollingKind::Last => vec!["--last".into()],
        };

        command.extend(vec![
            "--message-count".into(),
            format!("{}", self.message_count),
        ]);

        command.extend(match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        });

        command.push(match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        });

        command.push(format!("{}", self.partition_id));

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestMessagePollCmd {
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
                partitions_count: self.partitions_count,
                name: self.topic_name.clone(),
                message_expiry: None,
                max_topic_size: None,
                replication_factor: 1,
            })
            .await;
        assert!(topic.is_ok());

        let messages = self
            .messages
            .iter()
            .filter_map(|s| Message::from_str(s).ok())
            .collect::<Vec<_>>();

        let send_status = client
            .send_messages(&mut SendMessages {
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
                topic_id: Identifier::numeric(self.topic_id).unwrap(),
                partitioning: Partitioning::partition_id(self.partition_id),
                messages,
            })
            .await;
        assert!(send_status.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("message")
            .arg("poll")
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

        let message = format!("Executing poll messages from topic ID: {} and stream with ID: {}\nPolled messages from topic with ID: {} and stream with ID: {} (from partition with ID: {})\nPolled {} messages",
            topic_id, stream_id, topic_id, stream_id, self.partition_id, self.message_count);

        let status = command_state.success().stdout(starts_with(message));
        // Check if messages are printed based on the strategy
        match self.strategy.kind {
            PollingKind::Offset => {
                self.messages
                    .iter()
                    .skip(self.strategy.value as usize)
                    .take(self.message_count)
                    .fold(status, |status, message| status.stdout(contains(message)));
            }
            PollingKind::First => {
                self.messages
                    .iter()
                    .take(self.message_count)
                    .fold(status, |status, message| status.stdout(contains(message)));
            }
            PollingKind::Next => {
                self.messages
                    .iter()
                    .take(self.message_count)
                    .fold(status, |status, message| status.stdout(contains(message)));
            }
            PollingKind::Last => {
                self.messages
                    .iter()
                    .rev()
                    .take(self.message_count)
                    .fold(status, |status, message| status.stdout(contains(message)));
            }
            _ => {}
        }
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

    let test_messages: Vec<String> = vec![
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit".into(),
        "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua".into(),
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris".into(),
        "nisi ut aliquip ex ea commodo consequat".into(),
        "Duis aute irure dolor in reprehenderit in voluptate velit esse".into(),
        "cillum dolore eu fugiat nulla pariatur".into(),
        "Excepteur sint occaecat cupidatat non proident, sunt in culpa".into(),
        "qui officia deserunt mollit anim id est laborum".into(),
        "Sed ut perspiciatis unde omnis iste natus error sit voluptatem".into(),
        "accusantium doloremque laudantium, totam rem aperiam, eaque ipsa".into(),
    ];

    let test_parameters: Vec<(u32, usize, PollingStrategy, TestStreamId, TestTopicId)> = vec![
        (
            1,
            1,
            PollingStrategy::offset(0),
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ),
        (
            2,
            5,
            PollingStrategy::offset(0),
            TestStreamId::Numeric,
            TestTopicId::Named,
        ),
        (
            3,
            3,
            PollingStrategy::offset(3),
            TestStreamId::Named,
            TestTopicId::Numeric,
        ),
        (
            4,
            5,
            PollingStrategy::first(),
            TestStreamId::Named,
            TestTopicId::Named,
        ),
        (
            1,
            4,
            PollingStrategy::last(),
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ),
        (
            2,
            3,
            PollingStrategy::next(),
            TestStreamId::Numeric,
            TestTopicId::Named,
        ),
    ];

    iggy_cmd_test.setup().await;
    for (partition_id, message_count, strategy, using_stream_id, using_topic_id) in test_parameters
    {
        iggy_cmd_test
            .execute_test(TestMessagePollCmd::new(
                1,
                String::from("stream"),
                2,
                String::from("topic"),
                4,
                &test_messages,
                partition_id,
                message_count,
                strategy,
                using_stream_id,
                using_topic_id,
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
            vec!["message", "poll", "--help"],
            format!(
                r#"Poll messages from given topic ID and given stream ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples:
 iggy message poll --offset 0 1 2 1
 iggy message poll --offset 0 stream 2 1
 iggy message poll --offset 0 1 topic 1
 iggy message poll --offset 0 stream topic 1

{USAGE_PREFIX} message poll [OPTIONS] <--offset <OFFSET>|--first|--last|--next> <STREAM_ID> <TOPIC_ID> <PARTITION_ID>

Arguments:
  <STREAM_ID>
          ID of the stream from which message will be polled
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          ID of the topic from which message will be polled
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <PARTITION_ID>
          Partition ID from which message will be polled

Options:
  -m, --message-count <MESSAGE_COUNT>
          Number of messages to poll
{CLAP_INDENT}
          [default: 1]

  -a, --auto-commit
          Auto commit offset
{CLAP_INDENT}
          Flag indicates whether to commit offset on the server automatically
          after polling the messages.

  -o, --offset <OFFSET>
          Polling strategy - offset to start polling messages from
{CLAP_INDENT}
          Offset must be specified as a number

  -f, --first
          Polling strategy - start polling from the first message in the partition

  -l, --last
          Polling strategy - start polling from the last message in the partition

  -n, --next
          Polling strategy - start polling from the next message
{CLAP_INDENT}
          Start polling after the last polled message based
          on the stored consumer offset

  -c, --consumer <CONSUMER>
          Regular consumer which will poll messages
{CLAP_INDENT}
          Consumer ID can be specified as a consumer name or ID
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
            vec!["message", "poll", "-h"],
            format!(
                r#"Poll messages from given topic ID and given stream ID

{USAGE_PREFIX} message poll [OPTIONS] <--offset <OFFSET>|--first|--last|--next> <STREAM_ID> <TOPIC_ID> <PARTITION_ID>

Arguments:
  <STREAM_ID>     ID of the stream from which message will be polled
  <TOPIC_ID>      ID of the topic from which message will be polled
  <PARTITION_ID>  Partition ID from which message will be polled

Options:
  -m, --message-count <MESSAGE_COUNT>  Number of messages to poll [default: 1]
  -a, --auto-commit                    Auto commit offset
  -o, --offset <OFFSET>                Polling strategy - offset to start polling messages from
  -f, --first                          Polling strategy - start polling from the first message in the partition
  -l, --last                           Polling strategy - start polling from the last message in the partition
  -n, --next                           Polling strategy - start polling from the next message
  -c, --consumer <CONSUMER>            Regular consumer which will poll messages [default: 1]
  -h, --help                           Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
