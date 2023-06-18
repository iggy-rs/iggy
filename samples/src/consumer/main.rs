use anyhow::Result;
use clap::Parser;
use samples::shared::args::Args;
use samples::shared::messages::ORDER_CREATED_TYPE;
use samples::shared::messages::{Envelope, OrderCreated};
use sdk::client::Client;
use sdk::client_provider;
use sdk::client_provider::ClientProviderConfig;
use sdk::messages::poll_messages::{Format, Kind, PollMessages};
use sdk::streams::get_stream::GetStream;
use sdk::topics::get_topic::GetTopic;
use std::error::Error;
use tracing::{error, info};

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
    let stream_id = 9999;
    let topic_id = 1;
    let partition_id = 1;
    consume_messages(
        stream_id,
        topic_id,
        partition_id,
        args.consumer_id,
        args.interval,
        client,
    )
    .await
}

pub async fn consume_messages(
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
    consumer_id: u32,
    interval: u64,
    client: &dyn Client,
) -> Result<(), Box<dyn Error>> {
    validate_stream_and_topic(stream_id, topic_id, client).await;
    info!("Messages will be polled by consumer: {} from stream: {}, topic: {}, partition: {} with interval {} ms.", consumer_id, stream_id, topic_id, partition_id, interval);
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval));
    loop {
        let messages = client
            .poll_messages(&PollMessages {
                consumer_id,
                stream_id,
                topic_id,
                partition_id,
                kind: Kind::Next,
                value: 0,
                count: 1,
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
            let json = std::str::from_utf8(&message.payload)?;
            let envelope = serde_json::from_str::<Envelope>(json)?;
            match envelope.message_type.as_str() {
                ORDER_CREATED_TYPE => {
                    let order_created = serde_json::from_str::<OrderCreated>(&envelope.payload)?;
                    info!("Received {}: {:?}", ORDER_CREATED_TYPE, order_created);
                }
                _ => {
                    error!("Received unknown message type: {}", envelope.message_type);
                }
            }
        }
        interval.tick().await;
    }
}

async fn validate_stream_and_topic(stream_id: u32, topic_id: u32, client: &dyn Client) {
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
        if topic.is_ok() {
            info!("Topic: {} was found.", topic_id);
            break;
        }
        interval.tick().await;
    }
}
