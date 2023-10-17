use crate::cmd::common::{help::TestHelpCmd, IggyCmdTest, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["stream", "help"],
            format!(
                r#"stream operations

{USAGE_PREFIX} stream <COMMAND>

Commands:
  create  Create stream with given ID and name
  delete  Delete stream with given ID
  update  Update stream name for given stream ID
  get     Get details of a single stream with given ID
  list    List all streams
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
"#,
            ),
        ))
        .await;
}
