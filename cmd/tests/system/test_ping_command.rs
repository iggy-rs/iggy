use crate::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use predicates::str::{contains, starts_with};
use serial_test::serial;

struct TestPingCmd {
    count: usize,
}

impl Default for TestPingCmd {
    fn default() -> Self {
        Self { count: 3 }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestPingCmd {
    async fn prepare_server_state(&self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("ping")
            .arg("-c")
            .arg(format!("{}", self.count))
    }

    // Executing ping command
    // Ping sequence id:  1 time: 0.39 ms
    // Ping sequence id:  2 time: 0.69 ms
    // Ping sequence id:  3 time: 0.73 ms

    // Ping statistics for 3 ping commands
    // min/avg/max77/mdev = 0.393/0.618/0.746/0.116 ms

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with("Executing ping command\n"))
            .stdout(contains(format!(
                "Ping statistics for {} ping commands",
                self.count
            )));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[serial]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test.execute_test(TestPingCmd::default()).await;
}
