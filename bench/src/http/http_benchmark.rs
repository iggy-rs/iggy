use crate::args::Args;
use crate::benchmark::Benchmark;
use crate::test_result::TestResult;
use async_trait::async_trait;
use sdk::error::Error;
use sdk::http::client::Client;
use shared::messages::poll_messages::{Format, Kind, PollMessages};
use shared::messages::send_messages::{KeyKind, Message, SendMessages};
use shared::streams::create_stream::CreateStream;
use shared::topics::create_topic::CreateTopic;
use std::str::FromStr;
use std::time::Duration;
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, info};

const FROM_STREAM_ID: u32 = 1000000;

pub struct HttpBenchmark {}

#[async_trait]
impl Benchmark for HttpBenchmark {
    async fn poll_messages(&self, args: &Args) -> Vec<JoinHandle<TestResult>> {
        info!("Creating {} HTTP client(s)...", args.clients_count);
        let mut futures = Vec::with_capacity(args.clients_count as usize);
        let messages_per_batch = args.messages_per_batch;
        let message_batches = args.message_batches;
        for i in 0..args.clients_count {
            let client_id = i + 1;
            let client = Client::create(&args.http_api_url).unwrap();
            let clients_count = args.clients_count;
            let future = task::spawn(async move {
                info!("Executing the HTTP test on client #{}...", client_id);
                let result = Self::execute_poll_messages(
                    &client,
                    client_id,
                    messages_per_batch,
                    message_batches,
                    clients_count,
                )
                .await;

                match &result {
                    Ok(_) => info!(
                        "Executed the poll messages HTTP test on client #{}.",
                        client_id
                    ),
                    Err(error) => error!("Error on HTTP client #{}: {:?}", client_id, error),
                }

                result.unwrap()
            });
            futures.push(future);
        }
        info!("Created {} client(s).", args.clients_count);

        futures
    }

    async fn send_messages(&self, args: &Args) -> Vec<JoinHandle<TestResult>> {
        info!("Creating {} client(s)...", args.clients_count);
        let mut futures = Vec::with_capacity(args.clients_count as usize);
        let messages_per_batch = args.messages_per_batch;
        let message_batches = args.message_batches;
        let message_size = args.message_size;
        if message_size == 0 {
            panic!("Message size cannot be 0.")
        } else {
            info!("Message size: {} bytes.", message_size);
        }

        for i in 0..args.clients_count {
            let client_id = i + 1;
            let client = Client::create(&args.http_api_url).unwrap();
            let clients_count = args.clients_count;
            let future = task::spawn(async move {
                info!("Executing the HTTP test on client #{}...", client_id);
                let result = Self::execute_send_messages(
                    &client,
                    client_id,
                    messages_per_batch,
                    message_batches,
                    message_size,
                    clients_count,
                )
                .await;
                match &result {
                    Ok(_) => info!(
                        "Executed the send messages HTTP test on client #{}.",
                        client_id
                    ),
                    Err(error) => error!("Error on client #{}: {:?}", client_id, error),
                }

                result.unwrap()
            });
            futures.push(future);
        }
        info!("Created {} client(s).", args.clients_count);

        futures
    }
}

impl HttpBenchmark {
    pub(crate) async fn execute_poll_messages(
        client: &Client,
        client_id: u32,
        messages_per_batch: u32,
        batches_count: u32,
        clients_count: u32,
    ) -> Result<TestResult, Error> {
        let stream_id: u32 = FROM_STREAM_ID + client_id;
        let topic_id: u32 = 1;
        let partition_id: u32 = 1;
        let total_messages = messages_per_batch * batches_count;
        info!("client #{} → preparing the test messages...", client_id);

        let mut command = PollMessages {
            consumer_id: client_id,
            stream_id,
            topic_id,
            partition_id,
            kind: Kind::Offset,
            value: 0,
            count: messages_per_batch,
            auto_commit: false,
            format: Format::Binary,
        };

        info!(
            "client #{} → polling {} messages in {} batches of {} messages...",
            client_id, total_messages, batches_count, messages_per_batch
        );

        let mut latencies: Vec<Duration> = Vec::with_capacity(batches_count as usize);
        let start = Instant::now();

        let mut total_size_bytes = 0;
        for i in 0..batches_count {
            let offset = (i * messages_per_batch) as u64;
            let latency_start = Instant::now();
            command.value = offset;
            let messages = client.poll_messages(&command).await?;
            let latency_end = latency_start.elapsed();
            latencies.push(latency_end);
            for message in messages {
                total_size_bytes += message.get_size_bytes() as u64;
            }
        }

        let duration = start.elapsed() / clients_count;
        let average_latency = latencies.iter().sum::<Duration>().as_millis() as f64
            / ((clients_count * latencies.len() as u32) as f64);

        info!(
        "client #{} → polled {} messages in {} batches of {} messages in {} ms, total size: {} bytes, average latency: {:.2} ms.",
        client_id,
        total_messages,
        batches_count,
        messages_per_batch,
        duration.as_millis(),
        total_size_bytes,
        average_latency
    );

        Ok(TestResult {
            duration,
            average_latency,
            total_size_bytes,
        })
    }

    pub(crate) async fn execute_send_messages(
        client: &Client,
        client_id: u32,
        messages_per_batch: u32,
        batches_count: u32,
        message_size: u32,
        clients_count: u32,
    ) -> Result<TestResult, Error> {
        let stream_id: u32 = FROM_STREAM_ID + client_id;
        let topic_id: u32 = 1;
        let partition_id: u32 = 1;
        let partitions_count: u32 = 1;
        let stream_name = "test".to_string();
        let topic_name = "test".to_string();
        let total_messages = messages_per_batch * batches_count;

        info!("client #{} → getting the list of streams...", client_id);
        let streams = client.get_streams().await?;

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

        let payload = Self::create_payload(message_size);
        let mut messages = Vec::with_capacity(messages_per_batch as usize);
        for _ in 0..messages_per_batch {
            let message = Message::from_str(&payload).unwrap();
            messages.push(message);
        }

        let command = SendMessages {
            stream_id,
            topic_id,
            key_kind: KeyKind::PartitionId,
            key_value: partition_id,
            messages_count: messages_per_batch,
            messages,
        };

        info!(
            "client #{} → sending {} test messages in {} batches of {} messages...",
            client_id, total_messages, batches_count, messages_per_batch
        );

        let mut latencies: Vec<Duration> = Vec::with_capacity(batches_count as usize);
        let start = Instant::now();

        for _ in 0..batches_count {
            let latency_start = Instant::now();
            client.send_messages(&command).await?;
            let latency_end = latency_start.elapsed();
            latencies.push(latency_end);
        }

        let duration = start.elapsed() / clients_count;
        let average_latency = latencies.iter().sum::<Duration>().as_millis() as f64
            / ((clients_count * latencies.len() as u32) as f64);
        let total_size_bytes = (total_messages * message_size) as u64;

        info!(
        "client #{} → sent {} test messages in {} batches of {} messages in {} ms, total size: {} bytes, average latency: {:.2} ms.",
        client_id,
        total_messages,
        batches_count,
        messages_per_batch,
        duration.as_millis(),
        total_size_bytes,
        average_latency
    );

        Ok(TestResult {
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
}
