mod test_basic;
mod test_getting_started;
mod test_message_envelope;
mod test_message_headers;

use assert_cmd::Command;
use iggy::client::Client;
use iggy::client::StreamClient;
use iggy::client::SystemClient;
use iggy::client::TopicClient;
use iggy::client::UserClient;
use iggy::clients::client::IggyClient;
use iggy::clients::client::IggyClientBackgroundConfig;
use iggy::identifier::Identifier;
use iggy::streams::create_stream::CreateStream;
use iggy::system::ping::Ping;
use iggy::tcp::client::TcpClient;
use iggy::tcp::config::TcpClientConfig;
use iggy::topics::create_topic::CreateTopic;
use iggy::users::defaults::*;
use iggy::users::login_user::LoginUser;
use integration::test_server::{IpAddrKind, TestServer};
use regex::Regex;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct IggyExampleTest<'a> {
    server: TestServer,
    client: IggyClient,
    module: &'a str,
}

fn parse_sent_message(producer_stdout: &str) -> String {
    let re = Regex::new(r"(?s)Sent messages: \[\s*(.*?)\s*\]").unwrap();
    let mut captured_message = re
        .captures(producer_stdout)
        .and_then(|cap| cap.get(1).map(|match_| match_.as_str().to_string()))
        .unwrap();
    captured_message.pop(); // remove trailing `,`
    captured_message
}

fn verify_stdout_contains_expected_logs(
    producer_stdout: &str,
    consumer_stdout: &str,
    expected_producer_stdout: &Vec<&str>,
    expected_consumer_stdout: &Vec<&str>,
) {
    for (actual_output, expected_output) in [
        (producer_stdout, expected_producer_stdout),
        (consumer_stdout, expected_consumer_stdout),
    ] {
        for line in expected_output {
            assert!(
                actual_output.contains(line),
                "Output does not contain expected line: '{}'",
                line
            );
        }
    }
}

pub(crate) trait IggyExampleTestCase {
    fn verify_log_output(&self, producer_stdout: &str, consumer_stdout: &str);
    fn verify_message_output(&self, producer_stdout: &str, consumer_stdout: &str);
    fn verify_server_output(
        &self,
        producer_stdout: &str,
        consumer_stdout: &str,
        server: &TestServer,
    ) {
        let server_addr = server.get_raw_tcp_addr().unwrap();
        for stdout in [producer_stdout, consumer_stdout] {
            assert!(stdout.contains(
                format!("Iggy client is connecting to server: {}...", &server_addr).as_str()
            ));
            assert!(stdout.contains(
                format!("Iggy client has connected to server: {}", &server_addr).as_str()
            ));
        }
    }
    fn protocol(&self, server: &TestServer) -> Vec<String> {
        vec![
            "--tcp-server-address".into(),
            server.get_raw_tcp_addr().unwrap(),
        ]
    }
}

impl<'a> IggyExampleTest<'a> {
    pub(crate) fn new(module: &'a str) -> Self {
        let mut server = TestServer::new(None, true, None, IpAddrKind::V4);
        server.start();

        let tcp_client_config = TcpClientConfig {
            server_address: server.get_raw_tcp_addr().unwrap(),
            ..TcpClientConfig::default()
        };
        let client = Box::new(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
        let client = IggyClient::create(
            client,
            IggyClientBackgroundConfig::default(),
            None,
            None,
            None,
        );

        Self {
            server,
            client,
            module,
        }
    }

    pub(crate) async fn setup(&mut self, existing_stream_and_topic: bool) {
        self.client.connect().await.unwrap();
        let ping_result = self.client.ping(&Ping {}).await;
        assert!(ping_result.is_ok());
        self.client
            .login_user(&LoginUser {
                username: DEFAULT_ROOT_USERNAME.to_string(),
                password: DEFAULT_ROOT_PASSWORD.to_string(),
            })
            .await
            .unwrap();
        if existing_stream_and_topic {
            self.client
                .create_stream(&CreateStream {
                    stream_id: Some(1),
                    name: "sample-stream".to_string(),
                })
                .await
                .unwrap();
            self.client
                .create_topic(&CreateTopic {
                    stream_id: Identifier::numeric(1).unwrap(),
                    topic_id: Some(1),
                    partitions_count: 1,
                    name: "sample-topic".to_string(),
                    message_expiry: None,
                    max_topic_size: None,
                    replication_factor: 1,
                })
                .await
                .unwrap();
        }
    }

    pub(crate) async fn execute_test(&mut self, test_case: impl IggyExampleTestCase) {
        assert!(
            self.server.is_started(),
            "Server is not running, make sure it has been started with IggyExampleTest::setup()"
        );
        let (producer_stdout, consumer_stdout) = self
            .spawn_executables(test_case.protocol(&self.server))
            .await;

        test_case.verify_server_output(&producer_stdout, &consumer_stdout, &self.server);
        test_case.verify_log_output(&producer_stdout, &consumer_stdout);
        test_case.verify_message_output(&producer_stdout, &consumer_stdout);
    }
}

impl<'a> IggyExampleTest<'a> {
    async fn spawn_executables(&mut self, tcp_server_address: Vec<String>) -> (String, String) {
        let mut producer_cmd = Command::cargo_bin(format!("examples/{}-producer", self.module))
            .unwrap_or_else(|_| panic!("Failed to find {}-producer", self.module));
        let mut consumer_cmd = Command::cargo_bin(format!("examples/{}-consumer", self.module))
            .unwrap_or_else(|_| panic!("Failed to find {}-consumer", self.module));

        let mut args: Vec<String> = tcp_server_address.clone();
        args.push("--message-batches-limit".into());
        args.push("1".into());

        producer_cmd.args(args.clone());
        consumer_cmd.args(args);

        let producer_handle = tokio::spawn(async move {
            let producer_assert = producer_cmd
                .timeout(Duration::from_secs(10))
                .assert()
                .success();
            let producer_output = producer_assert.get_output();
            String::from_utf8_lossy(&producer_output.stdout)
                .as_ref()
                .to_string()
        });
        let consumer_handle = tokio::spawn(async move {
            let consumer_assert = consumer_cmd
                .timeout(Duration::from_secs(10))
                .assert()
                .success();
            let consumer_output = consumer_assert.get_output();
            String::from_utf8_lossy(&consumer_output.stdout)
                .as_ref()
                .to_string()
        });
        (
            producer_handle.await.unwrap(),
            consumer_handle.await.unwrap(),
        )
    }
}
