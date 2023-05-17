use crate::args::Args;
use crate::test_client::create_connected_client;
use sdk::client::ConnectedClient;
use sdk::error::Error;
use shared::messages::send_messages::{Message, SendMessages};
use shared::streams::create_stream::CreateStream;
use shared::streams::get_streams::GetStreams;
use shared::topics::create_topic::CreateTopic;
use std::str::FromStr;
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::info;

const FROM_STREAM_ID: u32 = 10000;

pub async fn init_send_messages(args: &Args) -> Result<Vec<JoinHandle<()>>, Error> {
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
        let client = create_connected_client(
            &args.client_address,
            &args.server_address,
            &args.server_name,
        )
        .await?;
        let future = task::spawn(async move {
            info!("Executing the test on client #{}...", client_id);
            let result = execute_send_messages(
                &client,
                client_id,
                messages_per_batch,
                message_batches,
                message_size,
            )
            .await;
            match result {
                Ok(_) => info!("Executed send messages the test on client #{}.", client_id),
                Err(error) => info!("Error on client #{}: {:?}", client_id, error),
            }
        });
        futures.push(future);
    }
    info!("Created {} client(s).", args.clients_count);

    Ok(futures)
}

async fn execute_send_messages(
    client: &ConnectedClient,
    client_id: u32,
    messages_per_batch: u32,
    batches_count: u32,
    message_size: u32,
) -> Result<(), Error> {
    let stream_id: u32 = FROM_STREAM_ID + client_id;
    let topic_id: u32 = 1;
    let partition_id: u32 = 1;
    let partitions_count: u32 = 1;
    let stream_name = "test".to_string();
    let topic_name = "test".to_string();
    let total_messages = messages_per_batch * batches_count;

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

    let payload = create_payload(message_size);
    let mut messages = Vec::with_capacity(messages_per_batch as usize);
    for _ in 0..messages_per_batch {
        let message = Message::from_str(&payload).unwrap();
        messages.push(message);
    }

    let command = SendMessages {
        stream_id,
        topic_id,
        key_kind: 0,
        key_value: partition_id,
        messages_count: messages_per_batch,
        messages,
    };

    info!(
        "client #{} → sending {} test messages in {} batches of {} messages...",
        client_id, total_messages, batches_count, messages_per_batch
    );

    let start = Instant::now();

    for _ in 0..batches_count {
        client.send_messages(&command).await?;
    }

    let duration = start.elapsed();

    info!(
        "client #{} → sent {} test messages in {} batches of {} messages in {} ms.",
        client_id,
        total_messages,
        batches_count,
        messages_per_batch,
        duration.as_millis(),
    );

    client.disconnect().await?;

    Ok(())
}

fn create_payload(size: u32) -> String {
    let mut payload = String::with_capacity(size as usize);
    for i in 0..size {
        let char = (i % 26 + 97) as u8 as char;
        payload.push(char);
    }

    payload
}
