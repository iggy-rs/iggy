use crate::test_args::TestArgs;
use sdk::client::ConnectedClient;
use sdk::error::Error;
use shared::messages::send_messages::{Message, SendMessages};
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::streams::get_streams::GetStreams;
use shared::topics::create_topic::CreateTopic;
use std::collections::HashMap;
use std::time::Instant;
use tracing::info;

pub async fn run_test(client: &mut ConnectedClient, args: TestArgs) -> Result<(), Error> {
    let stream_id: u32 = 9999;
    let topic_id: u32 = 1;
    let partition_id: u32 = 1;
    let partitions_count: u32 = 1;
    let stream_name = "test".to_string();
    let topic_name = "test".to_string();
    let messages_per_batch: u32 = args.messages_per_batch;
    let batches_count: u32 = args.message_batches;
    let total_messages = messages_per_batch * batches_count;

    info!("Getting the list of streams...");
    let streams = client.get_streams(&GetStreams {}).await?;

    if streams.iter().any(|s| s.id == stream_id) {
        info!("Deleting the test stream");
        if client
            .delete_stream(&DeleteStream { stream_id })
            .await
            .is_ok()
        {
            info!("Deleted the test stream.");
        }
    }

    info!("Creating the test stream...");
    client
        .create_stream(&CreateStream {
            stream_id,
            name: stream_name,
        })
        .await?;

    info!("Creating the test topic...");
    client
        .create_topic(&CreateTopic {
            stream_id,
            topic_id,
            partitions_count,
            name: topic_name,
        })
        .await?;

    info!("Preparing the test messages...");

    let mut message_number = 0;
    let mut message_batches: HashMap<u32, SendMessages> = HashMap::new();

    for i in 0..batches_count {
        let mut messages = Vec::with_capacity(messages_per_batch as usize);
        for _ in 0..messages_per_batch {
            let payload = format!("Test message #{}", message_number)
                .as_bytes()
                .to_vec();
            messages.push(Message {
                length: payload.len() as u32,
                payload,
            });
            message_number += 1;
        }

        let command = SendMessages {
            stream_id,
            topic_id,
            key_kind: 0,
            key_value: partition_id,
            messages_count: messages_per_batch,
            messages,
        };

        message_batches.insert(i, command);
    }

    info!(
        "Sending {} test messages in {} batches of {} messages...",
        total_messages, batches_count, messages_per_batch
    );

    let start = Instant::now();

    for i in 0..batches_count {
        let command = message_batches.get(&i).unwrap();
        client.send_messages(command).await?;
    }

    let duration = start.elapsed();

    info!(
        "Sent {} test messages in {} batches of {} messages in {} ms.",
        total_messages,
        batches_count,
        messages_per_batch,
        duration.as_millis()
    );

    Ok(())
}
