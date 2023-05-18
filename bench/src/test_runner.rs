use crate::args::Args;
use crate::poll_messages_test::init_poll_messages;
use crate::send_messages_test::init_send_messages;
use crate::test_result::TestResult;
use futures::future::join_all;
use sdk::error::Error;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::info;

pub async fn run_tests(args: Args) -> Result<(), Error> {
    info!("Starting the tests...");
    let total_messages =
        (args.messages_per_batch * args.message_batches * args.clients_count) as u64;
    if args.test_send_messages {
        info!(
            "Starting the send messages test for total amount of messages: {}...",
            total_messages
        );
        let test = init_send_messages(&args).await;
        execute(test, "send messages", total_messages).await;
    }
    if args.test_poll_messages {
        info!(
            "Starting the poll messages test for total amount of messages: {}...",
            total_messages
        );
        let test = init_poll_messages(&args).await;
        execute(test, "poll messages", total_messages).await;
    }
    info!("Finished the tests.");
    Ok(())
}

async fn execute(test: Vec<JoinHandle<TestResult>>, test_name: &str, total_messages: u64) {
    let results = join_all(test).await;
    let results = results
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<TestResult>>();

    let total_size_bytes = results.iter().map(|r| r.total_size_bytes).sum::<u64>();
    let total_duration = results.iter().map(|r| r.duration).sum::<Duration>();
    let average_latency = results.iter().map(|r| r.average_latency).sum::<f64>() / results.len() as f64;
    let average_throughput = total_size_bytes as f64 / total_duration.as_secs_f64() / 1024.0 / 1024.0;

    info!(
            "Finished the {} test for total amount of messages: {} in {} ms, total size: {} bytes, average latency: {:.2} ms, average throughput: {:.2} MB/s.",
            test_name, total_messages, total_duration.as_millis(), total_size_bytes, average_latency, average_throughput
        );
}
