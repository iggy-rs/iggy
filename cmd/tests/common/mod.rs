pub(crate) mod command;
pub(crate) mod server;

pub(crate) use crate::common::command::IggyCmdCommand;
use crate::common::server::TestServer;
use assert_cmd::assert::{Assert, OutputAssertExt};
use assert_cmd::prelude::CommandCargoExt;
use async_dropper::simple::AsyncDrop;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::client::{SystemClient, UserClient};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::system::ping::Ping;
use iggy::tcp::client::TcpClient;
use iggy::tcp::config::TcpClientConfig;
use iggy::users::defaults::*;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use std::process::Command;
use std::sync::Arc;

#[async_trait]
pub(crate) trait IggyCmdTestCase {
    async fn prepare_server_state(&self, client: &dyn Client);
    fn get_command(&self) -> IggyCmdCommand;
    fn verify_command(&self, command_state: Assert);
    async fn verify_server_state(&self, client: &dyn Client);
}

pub(crate) struct IggyCmdTest {
    server: TestServer,
    client: IggyClient,
}

impl IggyCmdTest {
    pub(crate) fn new() -> Self {
        let server = TestServer::default();
        let client = Box::new(TcpClient::create(Arc::new(TcpClientConfig::default())).unwrap());
        let client = IggyClient::create(client, IggyClientConfig::default(), None, None, None);

        Self { server, client }
    }

    pub(crate) async fn setup(&mut self) {
        self.server.start();
        self.client.connect().await.unwrap();

        let ping_result = self.client.ping(&Ping {}).await;

        assert!(ping_result.is_ok());

        let identity_info = self
            .client
            .login_user(&LoginUser {
                username: DEFAULT_ROOT_USERNAME.to_string(),
                password: DEFAULT_ROOT_PASSWORD.to_string(),
            })
            .await
            .unwrap();

        assert_eq!(identity_info.user_id, 1);
    }

    pub(crate) async fn execute_test(&mut self, test_case: impl IggyCmdTestCase) {
        // Make sure server is started
        assert!(
            self.server.is_started(),
            "Server is not running, make sure it has been started with IggyCmdTest::setup()"
        );
        // Prepare iggy server state before test
        test_case.prepare_server_state(&self.client).await;
        // Get iggy tool
        let mut command = Command::cargo_bin("iggy").unwrap();
        // Get command line arguments and environment variables from test case and execute command
        let command_args = test_case.get_command();
        // Set environment variables for the command
        command.envs(command_args.get_env());

        // When running action from github CI, binary needs to be started via QEMU.
        if let Ok(runner) = std::env::var("QEMU_RUNNER") {
            let mut runner_command = Command::new(runner);
            runner_command.arg(command.get_program().to_str().unwrap());
            runner_command.envs(command_args.get_env());
            command = runner_command;
        };

        // Execute test command
        let assert = command.args(command_args.get_opts_and_args()).assert();
        // Verify command output, exit code, etc in the test (if needed)
        test_case.verify_command(assert);
        // Verify iggy server state after the test
        test_case.verify_server_state(&self.client).await;
    }

    pub(crate) async fn teardown(&mut self) {
        let _ = self.client.logout_user(&LogoutUser {}).await;
    }
}

impl Default for IggyCmdTest {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncDrop for IggyCmdTest {
    async fn async_drop(&mut self) {
        self.teardown().await;
    }
}
