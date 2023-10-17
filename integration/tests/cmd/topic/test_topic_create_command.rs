use crate::cmd::common::{
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId, CLAP_INDENT,
    USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use humantime::format_duration;
use humantime::Duration as HumanDuration;
use iggy::streams::create_stream::CreateStream;
use iggy::topics::get_topic::GetTopic;
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
    using_identifier: TestStreamId,
}

impl TestTopicCreateCmd {
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        partitions_count: u32,
        message_expiry: Option<Vec<String>>,
        using_identifier: TestStreamId,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            partitions_count,
            message_expiry,
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
    async fn prepare_server_state(&self, client: &dyn Client) {
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
            .with_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let message_expiry_text = match self.message_expiry {
            Some(ref message_expiry) => {
                let duration: Duration =
                    *message_expiry.join(" ").parse::<HumanDuration>().unwrap();

                format!("and message expire time: {}", format_duration(duration))
            }
            None => String::from("and without message expire time"),
        };

        let stream_id = match self.using_identifier {
            TestStreamId::Numeric => format!("{}", self.stream_id),
            TestStreamId::Named => self.stream_name.clone(),
        };

        let message = format!("Executing create topic with ID: {}, name: {}, partitions count: {} {} in stream with ID: {}\nTopic with ID: {}, name: {}, partitions count: {} {} created in stream with ID: {}\n",
            self.topic_id, self.topic_name, self.partitions_count, message_expiry_text, stream_id, self.topic_id, self.topic_name, self.partitions_count, message_expiry_text, stream_id);

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
                String::from("1hour"),
                String::from("1min"),
                String::from("1sec"),
            ]),
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

{USAGE_PREFIX} topic create <STREAM_ID> <TOPIC_ID> <PARTITIONS_COUNT> <NAME> [MESSAGE_EXPIRY]...

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
          Message expiry time in human readable format like 15days 2min 2s ("none" or skipping parameter disables message expiry functionality in topic)

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
            vec!["topic", "create", "-h"],
            format!(
                r#"Create topic with given ID, name, number of partitions
and expiry time for given stream ID

{USAGE_PREFIX} topic create <STREAM_ID> <TOPIC_ID> <PARTITIONS_COUNT> <NAME> [MESSAGE_EXPIRY]...

Arguments:
  <STREAM_ID>          Stream ID to create topic
  <TOPIC_ID>           Topic ID to create
  <PARTITIONS_COUNT>   Number of partitions inside the topic
  <NAME>               Name of the topic
  [MESSAGE_EXPIRY]...  Message expiry time in human readable format like 15days 2min 2s ("none" or skipping parameter disables message expiry functionality in topic)

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
