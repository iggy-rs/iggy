use crate::cmd::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use predicates::str::{contains, starts_with};
use serial_test::serial;
use std::fmt::Display;

#[derive(Debug)]
enum Protocol {
    Default,
    Tcp,
    Quic,
}

impl Protocol {
    fn as_arg(&self) -> Vec<&str> {
        match self {
            Self::Default => vec![],
            Self::Tcp => vec!["--transport", "tcp"],
            Self::Quic => vec!["--transport", "quic"],
        }
    }
}

impl Default for Protocol {
    fn default() -> Self {
        Self::Default
    }
}

impl Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default => write!(f, "TCP"),
            Self::Tcp => write!(f, "TCP"),
            Self::Quic => write!(f, "QUIC"),
        }
    }
}

#[derive(Debug, Default)]
struct TestMeCmd {
    protocol: Protocol,
}

impl TestMeCmd {
    fn new(protocol: Protocol) -> Self {
        Self { protocol }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestMeCmd {
    async fn prepare_server_state(&self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .with_credentials()
            .opts(self.protocol.as_arg())
            .arg("me")
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with("Executing me command\n"))
            .stdout(contains(format!("Transport | {}", self.protocol)));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[serial]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test.execute_test(TestMeCmd::default()).await;
}

#[tokio::test]
#[serial]
pub async fn should_be_successful_using_transport_tcp() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestMeCmd::new(Protocol::Tcp))
        .await;
}

#[tokio::test]
#[serial]
pub async fn should_be_successful_using_transport_quic() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestMeCmd::new(Protocol::Quic))
        .await;
}
