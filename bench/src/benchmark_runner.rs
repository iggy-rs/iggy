use crate::args::Args;
use crate::benchmark;
use crate::benchmark::{BenchmarkKind, Transport};
use crate::benchmark_result::BenchmarkResult;
use crate::client_factory::ClientFactory;
use crate::http::HttpClientFactory;
use crate::quic::QuicClientFactory;
use futures::future::join_all;
use sdk::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

pub async fn run(args: Args) -> Result<(), Error> {
    info!("Starting the benchmarks...");
    let args = Arc::new(args);
    if args.http {
        start(args.clone(), Transport::Http, &HttpClientFactory {}).await;
    }
    if args.quic {
        start(args.clone(), Transport::Quic, &QuicClientFactory {}).await;
    }
    info!("Finished the benchmarks.");
    Ok(())
}

async fn start(args: Arc<Args>, transport: Transport, client_factory: &dyn ClientFactory) {
    if args.test_send_messages {
        execute(
            args.clone(),
            BenchmarkKind::SendMessages,
            transport,
            client_factory,
        )
        .await;
    }
    if args.test_poll_messages {
        execute(
            args.clone(),
            BenchmarkKind::PollMessages,
            transport,
            client_factory,
        )
        .await;
    }
}

async fn execute(
    args: Arc<Args>,
    kind: BenchmarkKind,
    transport: Transport,
    client_factory: &dyn ClientFactory,
) {
    let total_messages =
        (args.messages_per_batch * args.message_batches * args.clients_count) as u64;
    info!(
        "Starting the {} benchmark for: {}, total amount of messages: {}...",
        transport, kind, total_messages
    );

    let start_stream_id = match transport {
        Transport::Http => args.http_start_stream_id,
        Transport::Quic => args.quic_start_stream_id,
    };

    let results = benchmark::start(args.clone(), start_stream_id, client_factory, kind).await;
    let results = join_all(results).await;
    let results = results
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<BenchmarkResult>>();

    let total_size_bytes = results.iter().map(|r| r.total_size_bytes).sum::<u64>();
    let total_duration = results.iter().map(|r| r.duration).sum::<Duration>();
    let average_latency =
        results.iter().map(|r| r.average_latency).sum::<f64>() / results.len() as f64;
    let average_throughput =
        total_size_bytes as f64 / total_duration.as_secs_f64() / 1024.0 / 1024.0;

    info!(
            "Finished the {} benchmark for total amount of messages: {} in {} ms, total size: {} bytes, average latency: {:.2} ms, average throughput: {:.2} MB/s.",
            kind, total_messages, total_duration.as_millis(), total_size_bytes, average_latency, average_throughput
        );
}
