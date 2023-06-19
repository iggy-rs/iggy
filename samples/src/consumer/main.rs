use anyhow::Result;
use clap::Parser;
use samples::shared::args::Args;
use samples::shared::messages::*;
use sdk::client::Client;
use sdk::client_provider;
use sdk::client_provider::ClientProviderConfig;
use sdk::message::Message;
use sdk::messages::poll_messages::{Format, Kind, PollMessages};
use sdk::streams::get_stream::GetStream;
use sdk::topics::get_topic::GetTopic;
use std::error::Error;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Consumer has started, selected transport: {}",
        args.transport
    );
    let client_provider_config = ClientProviderConfig::from_args(args.to_sdk_args())?;
    let client = client_provider::get_client(client_provider_config).await?;
    let client = client.as_ref();
    consume_messages(&args, client).await
}

pub async fn consume_messages(args: &Args, client: &dyn Client) -> Result<(), Box<dyn Error>> {
    validate_system(args.stream_id, args.topic_id, args.partition_id, client).await;
    info!("Messages will be polled by consumer: {} from stream: {}, topic: {}, partition: {} with interval {} ms.",
        args.consumer_id, args.stream_id, args.topic_id, args.partition_id, args.interval);
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(args.interval));
    loop {
        let messages = client
            .poll_messages(&PollMessages {
                consumer_id: args.consumer_id,
                stream_id: args.stream_id,
                topic_id: args.topic_id,
                partition_id: args.partition_id,
                kind: Kind::Next,
                value: 0,
                count: args.messages_per_batch,
                auto_commit: true,
                format: Format::None,
            })
            .await?;
        if messages.is_empty() {
            info!("No messages found.");
            interval.tick().await;
            continue;
        }
        for message in messages {
            handle_message(&message)?;
        }
        interval.tick().await;
    }
}

fn handle_message(message: &Message) -> Result<(), Box<dyn Error>> {
    // The payload can be of any type as it is a raw byte array. In this case it's a JSON string.
    let json = std::str::from_utf8(&message.payload)?;
    // The message envelope can be used to send the different types of messages to the same topic.
    let envelope = serde_json::from_str::<Envelope>(json)?;
    info!(
        "Handling message type: {} at offset: {}...",
        envelope.message_type, message.offset
    );
    match envelope.message_type.as_str() {
        ORDER_CREATED_TYPE => {
            let order_created = serde_json::from_str::<OrderCreated>(&envelope.payload)?;
            info!("{:#?}", order_created);
        }
        ORDER_CONFIRMED_TYPE => {
            let order_confirmed = serde_json::from_str::<OrderConfirmed>(&envelope.payload)?;
            info!("{:#?}", order_confirmed);
        }
        ORDER_REJECTED_TYPE => {
            let order_rejected = serde_json::from_str::<OrderRejected>(&envelope.payload)?;
            info!("{:#?}", order_rejected);
        }
        _ => {
            warn!("Received unknown message type: {}", envelope.message_type);
        }
    }
    Ok(())
}

async fn validate_system(stream_id: u32, topic_id: u32, partition_id: u32, client: &dyn Client) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        info!("Validating if stream: {} exists..", stream_id);
        let stream = client.get_stream(&GetStream { stream_id }).await;
        if stream.is_ok() {
            info!("Stream: {} was found.", stream_id);
            break;
        }
        interval.tick().await;
    }
    loop {
        info!("Validating if topic: {} exists..", topic_id);
        let topic = client
            .get_topic(&GetTopic {
                stream_id,
                topic_id,
            })
            .await;
        if topic.is_err() {
            interval.tick().await;
            continue;
        }

        info!("Topic: {} was found.", topic_id);
        let topic = topic.unwrap();
        if topic.partitions_count >= partition_id {
            break;
        }

        panic!(
            "Topic: {} has only {} partition(s), but partition: {} was requested.",
            topic_id, topic.partitions_count, partition_id
        );
    }
}
