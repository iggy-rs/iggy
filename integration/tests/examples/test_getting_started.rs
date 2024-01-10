use super::verify_stdout_contains_expected_logs;
use crate::examples::{IggyExampleTest, IggyExampleTestCase};
use serial_test::serial;

static EXPECTED_CONSUMER_OUTPUT: [&str; 11] = [
    "Messages will be consumed from stream: 1, topic: 1, partition: 1 with interval 500 ms.",
    "Handling message at offset: 0, payload: message-1...",
    "Handling message at offset: 1, payload: message-2...",
    "Handling message at offset: 2, payload: message-3...",
    "Handling message at offset: 3, payload: message-4...",
    "Handling message at offset: 4, payload: message-5...",
    "Handling message at offset: 5, payload: message-6...",
    "Handling message at offset: 6, payload: message-7...",
    "Handling message at offset: 7, payload: message-8...",
    "Handling message at offset: 8, payload: message-9...",
    "Handling message at offset: 9, payload: message-10...",
];

struct TestGettingStarted<'a> {
    expected_producer_output: Vec<&'a str>,
    expected_consumer_output: Vec<&'a str>,
}

impl<'a> IggyExampleTestCase for TestGettingStarted<'a> {
    fn verify_log_output(&self, producer_stdout: &str, consumer_stdout: &str) {
        verify_stdout_contains_expected_logs(
            producer_stdout,
            consumer_stdout,
            &self.expected_producer_output,
            &self.expected_consumer_output,
        );
    }

    fn verify_message_output(&self, _producer_stdout: &str, _consumer_stdout: &str) {}
}

#[tokio::test]
#[serial]
async fn should_succeed_with_no_existing_stream_or_topic() {
    let mut iggy_example_test = IggyExampleTest::default();
    iggy_example_test.setup(false).await;

    iggy_example_test
        .execute_test(TestGettingStarted {
            expected_producer_output: vec![
                "Stream was created.",
                "Topic was created.",
                "Messages will be sent to stream: 1, topic: 1, partition: 1 with interval 500 ms.",
                "Sent 10 message(s).",
            ],
            expected_consumer_output: EXPECTED_CONSUMER_OUTPUT.to_vec(),
        })
        .await;
}

#[tokio::test]
#[serial]
async fn should_succeed_with_preexisting_stream_and_topic() {
    let mut iggy_example_test = IggyExampleTest::default();
    iggy_example_test.setup(true).await;

    iggy_example_test
        .execute_test(TestGettingStarted {
            expected_producer_output: vec![
                "Received an invalid response with status: 1011 (stream_id_already_exists).",
                "Stream already exists and will not be created again.",
                "Received an invalid response with status: 2012 (topic_id_already_exists).",
                "Topic already exists and will not be created again.",
                "Sent 10 message(s).",
            ],
            expected_consumer_output: EXPECTED_CONSUMER_OUTPUT.to_vec(),
        })
        .await;
}
