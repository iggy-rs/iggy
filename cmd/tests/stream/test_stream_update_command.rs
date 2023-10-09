use crate::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestStreamId};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::get_streams::GetStreams;
use predicates::str::diff;
use serial_test::serial;

struct TestStreamDeleteCmd {
    stream_id: u32,
    name: String,
    using_identifier: TestStreamId,
}

impl TestStreamDeleteCmd {
    fn new(stream_id: u32, name: String, using_identifier: TestStreamId) -> Self {
        Self {
            stream_id,
            name,
            using_identifier,
        }
    }

    fn to_arg(&self) -> String {
        match self.using_identifier {
            TestStreamId::Named => self.name.clone(),
            TestStreamId::Numeric => format!("{}", self.stream_id),
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamDeleteCmd {
    async fn prepare_server_state(&self, client: &dyn Client) {
        let stream = client
            .create_stream(&CreateStream {
                stream_id: self.stream_id,
                name: self.name.clone(),
            })
            .await;
        assert!(stream.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stream")
            .arg("delete")
            .arg(self.to_arg())
            .with_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let message = match self.using_identifier {
            TestStreamId::Named => format!(
                "Executing delete stream with ID: {}\nStream with ID: {} deleted\n",
                self.name, self.name
            ),
            TestStreamId::Numeric => format!(
                "Executing delete stream with ID: {}\nStream with ID: {} deleted\n",
                self.stream_id, self.stream_id
            ),
        };

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let streams = client.get_streams(&GetStreams {}).await;
        assert!(streams.is_ok());
        let streams = streams.unwrap();
        assert_eq!(streams.len(), 0);
    }
}

#[tokio::test]
#[serial]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamDeleteCmd::new(
            1,
            String::from("testing"),
            TestStreamId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamDeleteCmd::new(
            2,
            String::from("production"),
            TestStreamId::Named,
        ))
        .await;
}
