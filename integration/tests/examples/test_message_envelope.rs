use super::{parse_sent_message, verify_stdout_contains_expected_logs};
use crate::examples::{IggyExampleTest, IggyExampleTestCase};
use serial_test::serial;

struct TestMessageEnvelope<'a> {
    expected_producer_output: Vec<&'a str>,
    expected_consumer_output: Vec<&'a str>,
}

impl<'a> IggyExampleTestCase for TestMessageEnvelope<'a> {
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
        for line in producer_captured_message.lines() {
            let trimmed_line = line.trim();
            assert!(
                consumer_stdout.contains(trimmed_line),
                "Consumer output does not contain expected line: '{}'",
                trimmed_line
            );
        }
    }
}

#[tokio::test]
#[serial]
async fn should_successfully_execute() {
    let mut iggy_example_test = IggyExampleTest::new("message-envelope");
    iggy_example_test.setup(false).await;

    iggy_example_test
        .execute_test(TestMessageEnvelope {
            expected_producer_output: vec![
                "Message envelope producer has started, selected transport: tcp",
                "Iggy client is connecting to server: 127.0.0.1:8090...",
                "Iggy client has connected to server: 127.0.0.1:8090",
                "Received an invalid response with status: 1009 (stream_id_not_found).",
                "Stream does not exist, creating...",
                "Messages will be sent to stream: 9999, topic: 1, partition: 1 with interval 1000 ms.",
            ],
            expected_consumer_output: vec![
                "Message envelope consumer has started, selected transport: tcp",
                "Iggy client is connecting to server: 127.0.0.1:8090...",
                "Iggy client has connected to server: 127.0.0.1:8090",
                "Validating if stream: 9999 exists..",
                "Stream: 9999 was found.",
                "Validating if topic: 1 exists..",
                "Topic: 1 was found.",
                "Messages will be polled by consumer: 1 from stream: 9999, topic: 1, partition: 1 with interval 1000 ms.",
            ],
        })
        .await;
}
