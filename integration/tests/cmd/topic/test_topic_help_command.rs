use crate::cmd::common::{help::TestHelpCmd, IggyCmdTest, USAGE_PREFIX};
use serial_test::serial;

#[tokio::test]
#[serial]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "help"],
            format!(
                r#"topic operations

{USAGE_PREFIX} topic <COMMAND>

Commands:
  create  Create topic with given ID, name, number of partitions
              and expiry time for given stream ID
  delete  Delete topic with given ID in given stream ID
  update  Update topic name an message expiry time for given topic ID in given stream ID
  get     Get topic detail for given topic ID and stream ID
  list    List all topics in given stream ID
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
"#,
            ),
        ))
        .await;
}
