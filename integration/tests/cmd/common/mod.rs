pub(crate) mod command;
pub(crate) mod help;
pub(crate) use crate::cmd::common::command::IggyCmdCommand;
pub(crate) use crate::cmd::common::help::{TestHelpCmd, CLAP_INDENT, USAGE_PREFIX};
use assert_cmd::assert::{Assert, OutputAssertExt};
use assert_cmd::prelude::CommandCargoExt;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::client::{SystemClient, UserClient};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::system::ping::Ping;
use iggy::tcp::client::TcpClient;
use iggy::tcp::config::TcpClientConfig;
use iggy::users::defaults::*;
use iggy::users::login_user::LoginUser;
use integration::test_server::TestServer;
use std::fmt::{Display, Formatter, Result};
use std::process::Command;
use std::sync::Arc;

pub(crate) enum TestStreamId {
    Numeric,
    Named,
}

pub(crate) enum TestTopicId {
    Numeric,
    Named,
}

pub(crate) enum OutputFormat {
    Default,
    List,
    Table,
}

impl Display for OutputFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::Default => write!(f, "table"),
            Self::List => write!(f, "list"),
            Self::Table => write!(f, "table"),
        }
    }
}

impl OutputFormat {
    pub(crate) fn to_args(&self) -> Vec<&str> {
        match self {
            Self::Default => vec![],
            Self::List => vec!["--list-mode", "list"],
            Self::Table => vec!["--list-mode", "table"],
        }
    }
}

#[async_trait]
pub(crate) trait IggyCmdTestCase {
    async fn prepare_server_state(&self, client: &dyn Client);
    fn get_command(&self) -> IggyCmdCommand;
    fn verify_command(&self, command_state: Assert);
    async fn verify_server_state(&self, client: &dyn Client);
    fn protocol(&self, server: &TestServer) -> Vec<String> {
        vec![
            "--tcp-server-address".into(),
            server.get_raw_tcp_addr().unwrap(),
        ]
    }
}

pub(crate) struct IggyCmdTest {
    server: TestServer,
    client: IggyClient,
}

impl IggyCmdTest {
    pub(crate) fn new() -> Self {
        let server = TestServer::default();
        let tcp_client_config = TcpClientConfig {
            server_address: server.get_raw_tcp_addr().unwrap(),
            ..TcpClientConfig::default()
        };
        let client = Box::new(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
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
        // Set server address for the command - it's randomized for each test
        command.args(test_case.protocol(&self.server));

        // When running action from github CI, binary needs to be started via QEMU.
        if let Ok(runner) = std::env::var("QEMU_RUNNER") {
            let mut runner_command = Command::new(runner);
            runner_command.envs(command_args.get_env());
            runner_command.arg(command.get_program().to_str().unwrap());
            runner_command.args(test_case.protocol(&self.server));
            command = runner_command;
        };

        // Print used environment variables and command with all arguments.
        // By default, it will not be visible but once test is executed with
        // --nocapture flag, it will be visible.
        println!(
            "Running: {} {} {}",
            command
                .get_envs()
                .map(|k| format!(
                    "{}={}",
                    k.0.to_str().unwrap(),
                    k.1.unwrap().to_str().unwrap()
                ))
                .collect::<Vec<String>>()
                .join(" "),
            command.get_program().to_str().unwrap(),
            command_args.get_opts_and_args().join(" ")
        );

        // Execute test command
        let assert = command.args(command_args.get_opts_and_args()).assert();
        // Verify command output, exit code, etc in the test (if needed)
        test_case.verify_command(assert);
        // Verify iggy server state after the test
        test_case.verify_server_state(&self.client).await;
    }

    pub(crate) async fn execute_test_for_help_command(&mut self, test_case: TestHelpCmd) {
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

        // Print used environment variables and command with all arguments.
        // By default, it will not be visible but once test is executed with
        // --nocapture flag, it will be visible.
        println!(
            "Running: {} {} {}",
            command
                .get_envs()
                .map(|k| format!(
                    "{}={}",
                    k.0.to_str().unwrap(),
                    k.1.unwrap().to_str().unwrap()
                ))
                .collect::<Vec<String>>()
                .join(" "),
            command.get_program().to_str().unwrap(),
            command_args.get_opts_and_args().join(" ")
        );

        // Execute test command
        let assert = command.args(command_args.get_opts_and_args()).assert();
        // Verify command output, exit code, etc in the test (if needed)
        test_case.verify_command(assert);
    }
}

impl Default for IggyCmdTest {
    fn default() -> Self {
        Self::new()
    }
}
