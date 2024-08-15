use super::{parse_sent_message, verify_stdout_contains_expected_logs};
use crate::examples::{IggyExampleTest, IggyExampleTestCase};
use serial_test::parallel;

struct TestMessageHeaders<'a> {
    expected_producer_output: Vec<&'a str>,
    expected_consumer_output: Vec<&'a str>,
}

impl<'a> IggyExampleTestCase for TestMessageHeaders<'a> {
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
#[parallel]
async fn should_successfully_execute() {
    let mut iggy_example_test = IggyExampleTest::new("message-headers");
    iggy_example_test.setup(false).await;

    iggy_example_test
        .execute_test(TestMessageHeaders {
            expected_producer_output: vec![
                "Message headers producer has started, selected transport: tcp",
                "Stream does not exist, creating...",
                "Messages will be sent to stream: example-stream, topic: example-topic, partition: 1 with interval 1ms.",
            ],
            expected_consumer_output: vec![
                "Message headers consumer has started, selected transport: tcp",
                "Validating if stream: example-stream exists..",
                "Stream: example-stream was found.",
                "Validating if topic: example-topic exists..",
                "Topic: example-topic was found.",
                "Messages will be polled by consumer: 1 from stream: example-stream, topic: example-topic, partition: 1 with interval 1ms."
            ]
        })
        .await;
}
