use crate::cli::common::{help::TestHelpCmd, IggyCmdTest, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["message", "help"],
            format!(
                r#"message operations

{USAGE_PREFIX} message <COMMAND>

Commands:
  send   Send messages to given topic ID and given stream ID [aliases: s]
  poll   Poll messages from given topic ID and given stream ID [aliases: p]
  flush  Flush messages from given topic ID and given stream ID [aliases: f]
  help   Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
"#,
            ),
        ))
        .await;
}
