use crate::cli::common::{IggyCmdTest, TestHelpCmd, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["logout", "--help"],
            format!(
                r#"logout from Iggy server

Command logs out from Iggy server and removes session token from platform-specific
secure storage. After logout command is executed, user needs to log in again to
execute any command that requires authentication.

{USAGE_PREFIX} logout

Options:
  -h, --help
          Print help (see a summary with '-h')
"#,
            ),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_short_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["logout", "-h"],
            format!(
                r#"logout from Iggy server

{USAGE_PREFIX} logout

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
