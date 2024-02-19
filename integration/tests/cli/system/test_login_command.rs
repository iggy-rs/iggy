use crate::cli::common::{IggyCmdTest, TestHelpCmd, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["login", "--help"],
            format!(
                r#"login to Iggy server

Command logs in to Iggy server using provided credentials and stores session token
in platform-specific secure storage. Session token is used for authentication in
subsequent commands until logout command is executed.

{USAGE_PREFIX} login [EXPIRY]...

Arguments:
  [EXPIRY]...
          Login session expiry time in human-readable format
          
          Expiry time must be expressed in human-readable format like 1hour 15min 2s.
          If not set default value 15minutes is used. Using "none" disables session expiry time.

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
            vec!["login", "-h"],
            format!(
                r#"login to Iggy server

{USAGE_PREFIX} login [EXPIRY]...

Arguments:
  [EXPIRY]...  Login session expiry time in human-readable format

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
