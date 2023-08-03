use crate::args::Args;
use crate::benchmark::BenchmarkKind;
use crate::benchmark_result::BenchmarkResult;
use crate::client_factory::ClientFactory;
use iggy::error::Error;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Key, Message, SendMessages};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::info;

pub async fn run(
    client_factory: Arc<dyn ClientFactory>,
    producer_id: u32,
    args: Arc<Args>,
    stream_id: u32,
) -> Result<BenchmarkResult, Error> {
    let topic_id: u32 = 1;
    let partition_id: u32 = 1;
    let total_messages = (args.messages_per_batch * args.message_batches) as u64;
    let client = client_factory.create_client(args.clone()).await;
    info!("Producer #{} → preparing the test messages...", producer_id);
    let payload = create_payload(args.message_size);
    let mut messages = Vec::with_capacity(args.messages_per_batch as usize);
    for _ in 0..args.messages_per_batch {
        let message = Message::from_str(&payload).unwrap();
        messages.push(message);
    }

    let command = SendMessages {
        stream_id: Identifier::numeric(stream_id)?,
        topic_id: Identifier::numeric(topic_id)?,
        key: Key::partition_id(partition_id),
        messages,
    };

    info!(
        "Producer #{} → sending {} test messages in {} batches of {} messages...",
        producer_id, total_messages, args.message_batches, args.messages_per_batch
    );

    let mut latencies: Vec<Duration> = Vec::with_capacity(args.message_batches as usize);
    for _ in 0..args.message_batches {
        let latency_start = Instant::now();
        client.send_messages(&command).await?;
        let latency_end = latency_start.elapsed();
        latencies.push(latency_end);
    }

    let total_latencies = latencies.iter().sum::<Duration>();
    let duration = total_latencies / args.producers;
    let average_latency = latencies.iter().sum::<Duration>().as_millis() as f64
        / (args.producers * latencies.len() as u32) as f64;
    let total_size_bytes = total_messages * args.message_size as u64;
    let average_throughput = total_size_bytes as f64 / duration.as_secs_f64() / 1024.0 / 1024.0;

    info!(
    "Producer #{} → sent {} test messages in {} batches of {} messages in {} ms, total size: {} bytes, average latency: {:.2} ms, average throughput: {:.2} MB/s",
    producer_id,
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
        kind: BenchmarkKind::SendMessages,
    })
}

fn create_payload(size: u32) -> String {
    let mut payload = String::with_capacity(size as usize);
    for i in 0..size {
        let char = (i % 26 + 97) as u8 as char;
        payload.push(char);
    }

    payload
}
