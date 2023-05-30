use crate::args::Args;
use crate::benchmark::Benchmark;
use crate::http::http_benchmark::HttpBenchmark;
use crate::quic::quic_benchmark::QuicBenchmark;
use crate::test_result::TestResult;
use futures::future::join_all;
use sdk::error::Error;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::info;

pub async fn run_tests(args: Args) -> Result<(), Error> {
    info!("Starting the benchmarks...");
    let http_benchmark = HttpBenchmark {};
    let quic_benchmark = QuicBenchmark {};
    let total_messages =
        (args.messages_per_batch * args.message_batches * args.clients_count) as u64;
    if args.test_send_messages {
        let test_name = "send messages";
        info!(
            "Starting the {} benchmark for total amount of messages: {}...",
            test_name, total_messages
        );

        if args.http {
            let http_test = http_benchmark.send_messages(&args).await;
            execute(http_test, test_name, total_messages).await;
        }
        if args.quic {
            let quic_test = quic_benchmark.send_messages(&args).await;
            execute(quic_test, test_name, total_messages).await;
        }
    }
    if args.test_poll_messages {
        let test_name = "poll messages";
        info!(
            "Starting the {} benchmark for total amount of messages: {}...",
            test_name, total_messages
        );

        if args.http {
            let http_test = http_benchmark.poll_messages(&args).await;
            execute(http_test, test_name, total_messages).await;
        }
        if args.quic {
            let quic_test = quic_benchmark.poll_messages(&args).await;
            execute(quic_test, test_name, total_messages).await;
        }
    }
    info!("Finished the benchmarks.");
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
    let average_latency =
        results.iter().map(|r| r.average_latency).sum::<f64>() / results.len() as f64;
    let average_throughput =
        total_size_bytes as f64 / total_duration.as_secs_f64() / 1024.0 / 1024.0;

    info!(
            "Finished the {} test for total amount of messages: {} in {} ms, total size: {} bytes, average latency: {:.2} ms, average throughput: {:.2} MB/s.",
            test_name, total_messages, total_duration.as_millis(), total_size_bytes, average_latency, average_throughput
        );
}
