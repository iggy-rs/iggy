use crate::cmd::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use predicates::str::diff;
use serial_test::parallel;

struct TestNoCredentialsCmd {}

#[async_trait]
impl IggyCmdTestCase for TestNoCredentialsCmd {
    async fn prepare_server_state(&self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new().arg("me")
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .failure()
            .stderr(diff(
                "Error: CommandError(Iggy command line tool error\n\nCaused by:\n    Missing iggy server credentials)\n",
            ));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_fail_with_error_message() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test.execute_test(TestNoCredentialsCmd {}).await;
}
