use crate::cli::common::{help::TestHelpCmd, IggyCmdTest, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["consumer-group", "help"],
            format!(
                r#"consumer group operations

{USAGE_PREFIX} consumer-group <COMMAND>

Commands:
  create  Create consumer group with given ID and name for given stream ID and topic ID. [aliases: c]
  delete  Delete consumer group with given ID for given stream ID and topic ID [aliases: d]
  get     Get details of a single consumer group with given ID for given stream ID and topic ID [aliases: g]
  list    List all consumer groups for given stream ID and topic ID [aliases: l]
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
"#,
            ),
        ))
        .await;
}
