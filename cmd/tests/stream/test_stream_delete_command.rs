use crate::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::get_stream::GetStream;
use iggy::{client::Client, identifier::Identifier};
use predicates::str::diff;
use serial_test::serial;

enum TestStreamId {
    Numeric,
    Named,
}

struct TestStreamUpdateCmd {
    stream_id: u32,
    name: String,
    new_name: String,
    using_identifier: TestStreamId,
}

impl TestStreamUpdateCmd {
    fn new(stream_id: u32, name: String, new_name: String, using_identifier: TestStreamId) -> Self {
        Self {
            stream_id,
            name,
            new_name,
            using_identifier,
        }
    }

    fn to_args(&self) -> Vec<String> {
        match self.using_identifier {
            TestStreamId::Named => vec![self.name.clone(), self.new_name.clone()],
            TestStreamId::Numeric => {
                vec![format!("{}", self.stream_id), self.new_name.clone()]
            }
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamUpdateCmd {
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
            .arg("update")
            .args(self.to_args())
            .with_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let message = match self.using_identifier {
            TestStreamId::Named => format!("Executing update stream with ID: {} and name: {}\nStream with ID: {} updated name: {}\n", self.name, self.new_name, self.name, self.new_name),
            TestStreamId::Numeric => format!("Executing update stream with ID: {} and name: {}\nStream with ID: {} updated name: {}\n", self.stream_id, self.new_name, self.stream_id, self.new_name),
        };

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let stream = client
            .get_stream(&GetStream {
                stream_id: Identifier::numeric(self.stream_id).unwrap(),
            })
            .await;
        assert!(stream.is_ok());
        let stream = stream.unwrap();
        assert_eq!(stream.name, self.new_name);
    }
}

#[tokio::test]
#[serial]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamUpdateCmd::new(
            1,
            String::from("testing"),
            String::from("development"),
            TestStreamId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamUpdateCmd::new(
            2,
            String::from("production"),
            String::from("prototype"),
            TestStreamId::Named,
        ))
        .await;
}
