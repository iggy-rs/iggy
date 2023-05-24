use crate::args::Args;
use crate::test_client::create_connected_client;
use crate::test_result::TestResult;
use sdk::client::ConnectedClient;
use sdk::error::Error;
use shared::messages::poll_messages::{Format, Kind, PollMessages};
use std::time::Duration;
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, info};

const FROM_STREAM_ID: u32 = 10000;

pub async fn init_poll_messages(args: &Args) -> Vec<JoinHandle<TestResult>> {
    info!("Creating {} client(s)...", args.clients_count);
    let mut futures = Vec::with_capacity(args.clients_count as usize);
    let messages_per_batch = args.messages_per_batch;
    let message_batches = args.message_batches;
    for i in 0..args.clients_count {
        let client_id = i + 1;
        let client = create_connected_client(
            &args.client_address,
            &args.server_address,
            &args.server_name,
        )
        .await;
        if client.is_err() {
            panic!(
                "Error when creating client #{}: {:?}",
                client_id,
                client.err().unwrap()
            );
        }

        let client = client.unwrap();
        let clients_count = args.clients_count;
        let future = task::spawn(async move {
            info!("Executing the test on client #{}...", client_id);
            let result = execute_poll_messages(
                &client,
                client_id,
                messages_per_batch,
                message_batches,
                clients_count,
            )
            .await;

            match &result {
                Ok(_) => info!("Executed poll messages the test on client #{}.", client_id),
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
    client: &ConnectedClient,
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
