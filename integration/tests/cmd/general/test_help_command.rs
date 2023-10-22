use crate::cmd::common::{help::TestHelpCmd, IggyCmdTest, CLAP_INDENT, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["help"],
            format!(
                r#"Iggy is the persistent message streaming platform written in Rust, supporting QUIC, TCP and HTTP transport protocols, capable of processing millions of messages per second.

{USAGE_PREFIX} [OPTIONS] <COMMAND>

Commands:
  stream     stream operations
  topic      topic operations
  partition  partition operations
  ping       ping iggy server
  me         get current client info
  stats      get iggy server statistics
  pat        personal access token operations
  help       Print this message or the help of the given subcommand(s)

Options:
      --transport <TRANSPORT>
          [default: tcp]
      --encryption-key <ENCRYPTION_KEY>
          [default: ]
      --http-api-url <HTTP_API_URL>
          [default: http://localhost:3000]
      --http-retries <HTTP_RETRIES>
          [default: 3]
      --tcp-server-address <TCP_SERVER_ADDRESS>
          [default: 127.0.0.1:8090]
      --tcp-reconnection-retries <TCP_RECONNECTION_RETRIES>
          [default: 3]
      --tcp-reconnection-interval <TCP_RECONNECTION_INTERVAL>
          [default: 1000]
      --tcp-tls-enabled
{CLAP_INDENT}
      --tcp-tls-domain <TCP_TLS_DOMAIN>
          [default: localhost]
      --quic-client-address <QUIC_CLIENT_ADDRESS>
          [default: 127.0.0.1:0]
      --quic-server-address <QUIC_SERVER_ADDRESS>
          [default: 127.0.0.1:8080]
      --quic-server-name <QUIC_SERVER_NAME>
          [default: localhost]
      --quic-reconnection-retries <QUIC_RECONNECTION_RETRIES>
          [default: 3]
      --quic-reconnection-interval <QUIC_RECONNECTION_INTERVAL>
          [default: 1000]
      --quic-max-concurrent-bidi-streams <QUIC_MAX_CONCURRENT_BIDI_STREAMS>
          [default: 10000]
      --quic-datagram-send-buffer-size <QUIC_DATAGRAM_SEND_BUFFER_SIZE>
          [default: 100000]
      --quic-initial-mtu <QUIC_INITIAL_MTU>
          [default: 1200]
      --quic-send-window <QUIC_SEND_WINDOW>
          [default: 100000]
      --quic-receive-window <QUIC_RECEIVE_WINDOW>
          [default: 100000]
      --quic-response-buffer-size <QUIC_RESPONSE_BUFFER_SIZE>
          [default: 1048576]
      --quic-keep-alive-interval <QUIC_KEEP_ALIVE_INTERVAL>
          [default: 5000]
      --quic-max-idle-timeout <QUIC_MAX_IDLE_TIMEOUT>
          [default: 10000]
      --quic-validate-certificate
{CLAP_INDENT}
  -q, --quiet
          Quiet mode (disabled stdout printing)
  -d, --debug <DEBUG>
          Debug mode (verbose printing to given file)
  -u, --username <USERNAME>
          Iggy server username
  -p, --password <PASSWORD>
          Iggy server password
  -h, --help
          Print help
  -V, --version
          Print version
"#,
            ),
        ))
        .await;
}
