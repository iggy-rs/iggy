use crate::args::Args;
use crate::benchmark_result::BenchmarkResult;
use sdk::client::Client;
use sdk::error::Error;
use shared::messages::poll_messages::{Format, Kind, PollMessages};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::info;

pub async fn run(
    client: &dyn Client,
    client_id: u32,
    args: Arc<Args>,
    start_stream_id: u32,
) -> Result<BenchmarkResult, Error> {
    let stream_id: u32 = start_stream_id + client_id;
    let topic_id: u32 = 1;
    let partition_id: u32 = 1;
    let total_messages = args.messages_per_batch * args.message_batches;
    info!("client #{} → preparing the test messages...", client_id);

    let mut command = PollMessages {
        consumer_id: client_id,
        stream_id,
        topic_id,
        partition_id,
        kind: Kind::Offset,
        value: 0,
        count: args.messages_per_batch,
        auto_commit: false,
        format: Format::Binary,
    };

    info!(
        "client #{} → polling {} messages in {} batches of {} messages...",
        client_id, total_messages, args.message_batches, args.messages_per_batch
    );

    let mut latencies: Vec<Duration> = Vec::with_capacity(args.message_batches as usize);
    let start = Instant::now();

    let mut total_size_bytes = 0;
    for i in 0..args.message_batches {
        let offset = (i * args.messages_per_batch) as u64;
        let latency_start = Instant::now();
        command.value = offset;
        let messages = client.poll_messages(&command).await?;
        let latency_end = latency_start.elapsed();
        latencies.push(latency_end);
        for message in messages {
            total_size_bytes += message.get_size_bytes() as u64;
        }
    }

    let duration = start.elapsed() / args.clients_count;
    let average_latency = latencies.iter().sum::<Duration>().as_millis() as f64
        / ((args.clients_count * latencies.len() as u32) as f64);

    info!(
    "client #{} → polled {} messages in {} batches of {} messages in {} ms, total size: {} bytes, average latency: {:.2} ms.",
    client_id,
    total_messages,
    args.message_batches,
    args.messages_per_batch,
    duration.as_millis(),
    total_size_bytes,
    average_latency
);

    Ok(BenchmarkResult {
        duration,
        average_latency,
        total_size_bytes,
    })
}
