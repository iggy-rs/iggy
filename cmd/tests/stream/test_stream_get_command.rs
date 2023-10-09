use crate::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestStreamId};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::streams::create_stream::CreateStream;
use predicates::str::{contains, starts_with};
use serial_test::serial;

struct TestStreamGetCmd {
    stream_id: u32,
    name: String,
    using_identifier: TestStreamId,
}

impl TestStreamGetCmd {
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
impl IggyCmdTestCase for TestStreamGetCmd {
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
            .arg("get")
            .arg(self.to_arg())
            .with_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let start_message = match self.using_identifier {
            TestStreamId::Named => format!("Executing get stream with ID: {}\n", self.name.clone()),
            TestStreamId::Numeric => format!("Executing get stream with ID: {}\n", self.stream_id),
        };

        command_state
            .success()
            .stdout(starts_with(start_message))
            .stdout(contains(format!(
                "Stream ID            | {}",
                self.stream_id
            )))
            .stdout(contains(format!("Stream name          | {}", self.name)))
            .stdout(contains("Stream size          | 0"))
            .stdout(contains("Stream message count | 0"))
            .stdout(contains("Stream topics count  | 0"));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[serial]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamGetCmd::new(
            1,
            String::from("production"),
            TestStreamId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamGetCmd::new(
            2,
            String::from("testing"),
            TestStreamId::Numeric,
        ))
        .await;
}
