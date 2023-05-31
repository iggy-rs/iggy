use crate::args::Args;
use crate::benchmark_result::BenchmarkResult;
use crate::client_factory::ClientFactory;
use sdk::client::Client;
use sdk::error::Error;
use shared::messages::poll_messages::{Format, Kind, PollMessages};
use shared::messages::send_messages::{KeyKind, Message, SendMessages};
use shared::streams::create_stream::CreateStream;
use shared::streams::get_streams::GetStreams;
use shared::topics::create_topic::CreateTopic;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, info};

#[derive(Debug, Clone, Copy)]
pub enum BenchmarkKind {
    SendMessages,
    PollMessages,
}

#[derive(Debug, Clone, Copy)]
pub enum Transport {
    Http,
    Quic,
}

impl Display for BenchmarkKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchmarkKind::SendMessages => write!(f, "send messages"),
            BenchmarkKind::PollMessages => write!(f, "poll messages"),
        }
    }
}

impl Display for Transport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Transport::Http => write!(f, "HTTP"),
            Transport::Quic => write!(f, "QUIC"),
        }
    }
}

pub async fn start(
    args: Arc<Args>,
    start_stream_id: u32,
    client_factory: &dyn ClientFactory,
    kind: BenchmarkKind,
) -> Vec<JoinHandle<BenchmarkResult>> {
    info!("Creating {} client(s)...", args.clients_count);
    let mut futures = Vec::with_capacity(args.clients_count as usize);
    for i in 0..args.clients_count {
        let client_id = i + 1;
        let args = args.clone();
        let client = client_factory.create_client(args.clone()).await;
        let future = task::spawn(async move {
            info!("Executing the test on client #{}...", client_id);
            let args = args.clone();
            let result = match kind {
                BenchmarkKind::SendMessages => {
                    execute_send_messages(client.as_ref(), client_id, args, start_stream_id).await
                }
                BenchmarkKind::PollMessages => {
                    execute_poll_messages(client.as_ref(), client_id, args, start_stream_id).await
                }
            };
            match &result {
                Ok(_) => info!("Executed the test on client #{}.", client_id),
                Err(error) => error!("Error on client #{}: {:?}", client_id, error),
            }

            result.unwrap()
        });
        futures.push(future);
    }
    info!("Created {} client(s).", args.clients_count);

    futures
}

async fn execute_poll_messages(
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

async fn execute_send_messages(
    client: &dyn Client,
    client_id: u32,
    args: Arc<Args>,
    start_stream_id: u32,
) -> Result<BenchmarkResult, Error> {
    let stream_id: u32 = start_stream_id + client_id;
    let topic_id: u32 = 1;
    let partition_id: u32 = 1;
    let partitions_count: u32 = 1;
    let stream_name = "test".to_string();
    let topic_name = "test".to_string();
    let total_messages = args.messages_per_batch * args.message_batches;

    info!("client #{} → getting the list of streams...", client_id);
    let streams = client.get_streams(&GetStreams {}).await?;

    if streams.iter().all(|s| s.id != stream_id) {
        info!("client #{} → creating the test stream...", client_id);
        client
            .create_stream(&CreateStream {
                stream_id,
                name: stream_name,
            })
            .await?;

        info!("client #{} → creating the test topic...", client_id);
        client
            .create_topic(&CreateTopic {
                stream_id,
                topic_id,
                partitions_count,
                name: topic_name,
            })
            .await?;
    }

    info!("client #{} → preparing the test messages...", client_id);

    let payload = create_payload(args.message_size);
    let mut messages = Vec::with_capacity(args.messages_per_batch as usize);
    for _ in 0..args.messages_per_batch {
        let message = Message::from_str(&payload).unwrap();
        messages.push(message);
    }

    let command = SendMessages {
        stream_id,
        topic_id,
        key_kind: KeyKind::PartitionId,
        key_value: partition_id,
        messages_count: args.messages_per_batch,
        messages,
    };

    info!(
        "client #{} → sending {} test messages in {} batches of {} messages...",
        client_id, total_messages, args.message_batches, args.messages_per_batch
    );

    let mut latencies: Vec<Duration> = Vec::with_capacity(args.message_batches as usize);
    let start = Instant::now();

    for _ in 0..args.message_batches {
        let latency_start = Instant::now();
        client.send_messages(&command).await?;
        let latency_end = latency_start.elapsed();
        latencies.push(latency_end);
    }

    let duration = start.elapsed() / args.clients_count;
    let average_latency = latencies.iter().sum::<Duration>().as_millis() as f64
        / (args.clients_count * latencies.len() as u32) as f64;
    let total_size_bytes = (total_messages * args.message_size) as u64;

    info!(
    "client #{} → sent {} test messages in {} batches of {} messages in {} ms, total size: {} bytes, average latency: {:.2} ms.",
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

fn create_payload(size: u32) -> String {
    let mut payload = String::with_capacity(size as usize);
    for i in 0..size {
        let char = (i % 26 + 97) as u8 as char;
        payload.push(char);
    }

    payload
}
