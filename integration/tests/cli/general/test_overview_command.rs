use crate::cli::common::{help::TestHelpCmd, IggyCmdTest};
use serial_test::parallel;

const FIGLET_INDENT: &str = " ";
const FIGLET_FILL: &str = "                         ";

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();
    let no_arg: Vec<String> = vec![];

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            no_arg,
            format!(
                r#"  ___                              ____   _       ___{FIGLET_INDENT}
 |_ _|   __ _    __ _   _   _     / ___| | |     |_ _|
  | |   / _` |  / _` | | | | |   | |     | |      | |{FIGLET_INDENT}
  | |  | (_| | | (_| | | |_| |   | |___  | |___   | |{FIGLET_INDENT}
 |___|  \__, |  \__, |  \__, |    \____| |_____| |___|
        |___/   |___/   |___/{FIGLET_FILL}

Iggy is the persistent message streaming platform written in Rust, supporting QUIC, TCP and HTTP transport protocols, capable of processing millions of messages per second.

Usage: iggy [OPTIONS] [COMMAND]

Commands:
  stream           stream operations [aliases: s]
  topic            topic operations [aliases: t]
  partition        partition operations [aliases: p]
  ping             ping iggy server
  me               get current client info
  stats            get iggy server statistics
  pat              personal access token operations
  user             user operations [aliases: u]
  client           client operations [aliases: c]
  consumer-group   consumer group operations [aliases: g]
  consumer-offset  consumer offset operations [aliases: o]
  message          message operations [aliases: m]
  context          context operations [aliases: ctx]
  help             Print this message or the help of the given subcommand(s)


Run 'iggy --help' for full help message.
Run 'iggy COMMAND --help' for more information on a command.

For more help on what's Iggy and how to use it, head to https://iggy.rs
"#,
            ),
        ))
        .await;
}
