mod common;
use crate::common::quic::QuicClientFactory;
use crate::common::test_runner;

#[tokio::test]
async fn stream_should_be_created_and_messages_should_be_appended_to_the_partition() {
    let client_factory = QuicClientFactory {};
    test_runner::run(&client_factory).await;
}
