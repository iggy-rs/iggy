use crate::cli::common::{help::TestHelpCmd, IggyCmdTest, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["pat", "help"],
            format!(
                r#"personal access token operations

{USAGE_PREFIX} pat <COMMAND>

Commands:
  create  Create personal access token [aliases: c]
  delete  Delete personal access token [aliases: d]
  list    List all personal access tokens [aliases: l]
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
"#,
            ),
        ))
        .await;
}
