use super::{parse_sent_message, verify_stdout_contains_expected_logs};
use crate::examples::{IggyExampleTest, IggyExampleTestCase};
use serial_test::parallel;

struct TestBasic<'a> {
    expected_producer_output: Vec<&'a str>,
    expected_consumer_output: Vec<&'a str>,
}

impl<'a> IggyExampleTestCase for TestBasic<'a> {
    fn verify_log_output(&self, producer_stdout: &str, consumer_stdout: &str) {
        verify_stdout_contains_expected_logs(
            producer_stdout,
            consumer_stdout,
            &self.expected_producer_output,
            &self.expected_consumer_output,
        );
    }

    fn verify_message_output(&self, producer_stdout: &str, consumer_stdout: &str) {
        let producer_captured_message = parse_sent_message(producer_stdout);
        // remove leading + trailing `,` and add trailing elipses
        let producer_formatted_message = format!(
            "{}...",
            &producer_captured_message[1..producer_captured_message.len() - 1]
        );
        assert!(
            consumer_stdout.contains(&producer_formatted_message),
            "Consumer output does not contain expected line: '{}'",
            &producer_formatted_message
        );
    }
}

#[tokio::test]
#[parallel]
async fn should_successfully_execute() {
    let mut iggy_example_test = IggyExampleTest::new("basic");
    iggy_example_test.setup(false).await;

    iggy_example_test
        .execute_test(TestBasic {
            expected_producer_output: vec![
                "Basic producer has started, selected transport: tcp",
                "Received an invalid response with status: 1009 (stream_id_not_found).",
                "Stream does not exist, creating...",
                "Messages will be sent to stream: 9999, topic: 1, partition: 1 with interval 1000 ms.",
            ],
            expected_consumer_output: vec![
                "Basic consumer has started, selected transport: tcp",
                "Validating if stream: 9999 exists..",
                "Stream: 9999 was found.",
                "Validating if topic: 1 exists..",
                "Topic: 1 was found.",
                "Messages will be polled by consumer: 1 from stream: 9999, topic: 1, partition: 1 with interval 1000 ms."
            ],
        })
        .await;
}
