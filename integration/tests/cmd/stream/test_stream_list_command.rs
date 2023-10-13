use crate::cmd::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, OutputFormat};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::streams::create_stream::CreateStream;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestStreamListCmd {
    stream_id: u32,
    name: String,
    output: OutputFormat,
}

impl TestStreamListCmd {
    fn new(stream_id: u32, name: String, output: OutputFormat) -> Self {
        Self {
            stream_id,
            name,
            output,
        }
    }

    fn to_args(&self) -> Vec<&str> {
        self.output.to_args()
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamListCmd {
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
            .arg("list")
            .args(self.to_args())
            .with_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with(format!(
                "Executing list streams in {} mode",
                self.output
            )))
            .stdout(contains(self.name.clone()));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamListCmd::new(
            1,
            String::from("prod"),
            OutputFormat::Default,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamListCmd::new(
            2,
            String::from("testing"),
            OutputFormat::List,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamListCmd::new(
            3,
            String::from("misc"),
            OutputFormat::Table,
        ))
        .await;
}
