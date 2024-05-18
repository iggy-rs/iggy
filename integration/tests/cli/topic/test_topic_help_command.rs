use crate::cli::common::{help::TestHelpCmd, IggyCmdTest, USAGE_PREFIX};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "help"],
            format!(
                r#"topic operations

{USAGE_PREFIX} topic <COMMAND>

Commands:
  create  Create topic with given name, number of partitions, compression algorithm and expiry time for given stream ID [aliases: c]
  delete  Delete topic with given ID in given stream ID [aliases: d]
  update  Update topic name, compression algorithm and message expiry time for given topic ID in given stream ID [aliases: u]
  get     Get topic detail for given topic ID and stream ID [aliases: g]
  list    List all topics in given stream ID [aliases: l]
  purge   Purge topic with given ID in given stream ID [aliases: p]
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
"#,
            ),
        ))
        .await;
}
