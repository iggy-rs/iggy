use crate::cmd::common::{help::TestHelpCmd, IggyCmdTest, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["user", "help"],
            format!(
                r#"user operations

{USAGE_PREFIX} user <COMMAND>

Commands:
  create       Create user with given username and password
  delete       Delete user with given ID
  get          Get details of a single user with given ID
  list         List all users
  name         Change username for user with given ID
  status       Change status for user with given ID
  password     Change password for user with given ID
  permissions  Set permissions for user with given ID
  help         Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
"#,
            ),
        ))
        .await;
}
