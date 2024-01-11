use crate::cli::common::{help::TestHelpCmd, IggyCmdTest, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["client", "help"],
            format!(
                r#"client operations

{USAGE_PREFIX} client <COMMAND>

Commands:
  get   Get details of a single client with given ID [aliases: g]
  list  List all currently connected clients to iggy server [aliases: l]
  help  Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
"#,
            ),
        ))
        .await;
}
