use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::next_client::ClientNext;
use predicates::str::diff;
use serial_test::parallel;

struct TestQuietModCmd {}

#[async_trait]
impl IggyCmdTestCase for TestQuietModCmd {
    async fn prepare_server_state(&mut self, _client: &dyn ClientNext) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new().arg("ping").opt("-q")
    }

    fn verify_command(&self, command_state: Assert) {
        command_state.success().stdout(diff(""));
    }

    async fn verify_server_state(&self, _client: &dyn ClientNext) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_no_output() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test.execute_test(TestQuietModCmd {}).await;
}
