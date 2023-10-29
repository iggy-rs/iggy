use crate::cmd::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use integration::test_server::TestServer;
use predicates::str::{contains, starts_with};
use serial_test::parallel;
use std::fmt::Display;

#[derive(Debug)]
enum Protocol {
    Tcp,
    Quic,
}

impl Protocol {
    fn as_arg(&self) -> Vec<&str> {
        match self {
            Self::Tcp => vec!["--transport", "tcp"],
            Self::Quic => vec!["--transport", "quic"],
        }
    }
}

impl Default for Protocol {
    fn default() -> Self {
        Self::Tcp
    }
}

impl Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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

    fn protocol(&self, server: &TestServer) -> Vec<String> {
        match &self.protocol {
            Protocol::Tcp => vec![
                "--tcp-server-address".into(),
                server.get_raw_tcp_addr().unwrap(),
            ],
            Protocol::Quic => vec![
                "--quic-server-address".into(),
                server.get_quic_udp_addr().unwrap(),
            ],
        }
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test.execute_test(TestMeCmd::default()).await;
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful_using_transport_tcp() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestMeCmd::new(Protocol::Tcp))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful_using_transport_quic() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestMeCmd::new(Protocol::Quic))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["me", "--help"],
            format!(
                r#"get current client info

Command connects to Iggy server and collects client info like client ID, user ID server address and protocol type.

{USAGE_PREFIX} me

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
            vec!["me", "-h"],
            format!(
                r#"get current client info

{USAGE_PREFIX} me

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
