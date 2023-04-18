use crate::client_error::ClientError;
use sdk::client::Client;
use shared::messages::send_message::SendMessage;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::topics::create_topic::CreateTopic;
use tracing::info;

pub async fn run_test(client: &mut Client) -> Result<(), ClientError> {
    let stream_id: u32 = 9999;
    let topic_id: u32 = 1;
    let partitions_count: u32 = 1;
    let stream_name = "test".to_string();
    let topic_name = "test".to_string();
    let messages_count = 1000;

    info!("Deleting the test stream (if exists)...");
    if client
        .delete_stream(&DeleteStream { stream_id })
        .await
        .is_ok()
    {
        info!("Deleted the test stream.");
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

    info!("Sending {} test messages...", messages_count);
    for i in 1..messages_count {
        let payload = format!("Test message #{}", i).as_bytes().to_vec();
        let command = SendMessage {
            stream_id,
            topic_id,
            key_kind: 0,
            key_value: 0,
            payload,
        };
        client.send_message(&command).await?;
    }

    info!("Finished sending {} test messages.", messages_count);

    Ok(())
}
