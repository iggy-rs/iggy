use crate::client_error::ClientError;
use sdk::client::ConnectedClient;
use shared::messages::send_messages::{Message, SendMessages};
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::topics::create_topic::CreateTopic;
use tracing::info;

pub async fn run_test(client: &mut ConnectedClient) -> Result<(), ClientError> {
    let stream_id: u32 = 9999;
    let topic_id: u32 = 1;
    let partitions_count: u32 = 1;
    let stream_name = "test".to_string();
    let topic_name = "test".to_string();
    let messages_count: u32 = 10000;

    info!("Getting the list of streams...");
    let streams = client.get_streams().await?;

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

    let mut messages = Vec::with_capacity(messages_count as usize);

    for i in 0..messages_count {
        let payload = format!("Test message #{}", i).as_bytes().to_vec();
        messages.push(Message {
            length: payload.len() as u32,
            payload,
        });
    }

    info!("Sending {} test messages...", messages_count);

    let command = SendMessages {
        stream_id,
        topic_id,
        key_kind: 0,
        key_value: 0,
        messages_count,
        messages,
    };
    client.send_messages(&command).await?;

    info!("Finished sending {} test messages.", messages_count);

    Ok(())
}
