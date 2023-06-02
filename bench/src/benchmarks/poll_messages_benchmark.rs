use crate::args::Args;
use crate::benchmark_result::BenchmarkResult;
use crate::client_factory::ClientFactory;
use sdk::error::Error;
use shared::messages::poll_messages::{Format, Kind, PollMessages};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, trace};

pub async fn run(
    client_factory: Arc<dyn ClientFactory>,
    consumer_id: u32,
    args: Arc<Args>,
    stream_id: u32,
) -> Result<BenchmarkResult, Error> {
    let topic_id: u32 = 1;
    let partition_id: u32 = 1;
    let total_messages = args.messages_per_batch * args.message_batches;
    let client = client_factory.create_client(args.clone()).await;
    info!("Consumer #{} → preparing the test messages...", consumer_id);
    let mut command = PollMessages {
        consumer_id,
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
        "Consumer #{} → polling {} messages in {} batches of {} messages...",
        consumer_id, total_messages, args.message_batches, args.messages_per_batch
    );

    let mut latencies: Vec<Duration> = Vec::with_capacity(args.message_batches as usize);
    let start = Instant::now();

    let mut total_size_bytes = 0;
    let mut current_iteration = 0;
    let mut received_messages = 0;
    while received_messages < total_messages {
        let offset = (current_iteration * args.messages_per_batch) as u64;
        let latency_start = Instant::now();
        command.value = offset;
        let messages = client.poll_messages(&command).await;
        if messages.is_err() {
            trace!("Offset: {} is not available yet, retrying...", offset);
            continue;
        }

        let messages = messages.unwrap();
        if messages.is_empty() {
            trace!("Messages are empty for offset: {}, retrying...", offset);
            continue;
        }

        let latency_end = latency_start.elapsed();
        received_messages += messages.len() as u32;
        latencies.push(latency_end);
        for message in messages {
            total_size_bytes += message.get_size_bytes() as u64;
        }
        current_iteration += 1;
    }

    let duration = start.elapsed() / args.consumers;
    let average_latency = latencies.iter().sum::<Duration>().as_millis() as f64
        / ((args.consumers * latencies.len() as u32) as f64);
    let average_throughput = total_size_bytes as f64 / duration.as_secs_f64() / 1024.0 / 1024.0;

    info!(
    "Consumer #{} → polled {} messages in {} batches of {} messages in {} ms, total size: {} bytes, average latency: {:.2} ms, average throughput: {:.2} MB/s",
    consumer_id,
    total_messages,
    args.message_batches,
    args.messages_per_batch,
    duration.as_millis(),
    total_size_bytes,
    average_latency,
    average_throughput
);

    Ok(BenchmarkResult {
        duration,
        average_latency,
        total_size_bytes,
    })
}
