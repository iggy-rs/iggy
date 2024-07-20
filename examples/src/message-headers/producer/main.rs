use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use iggy::client::MessageClient;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::IggyClient;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy_examples::shared::args::Args;
use iggy_examples::shared::messages_generator::MessagesGenerator;
use iggy_examples::shared::system;
use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Message headers producer has started, selected transport: {}",
        args.transport
    );
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.to_sdk_args())?);
    let client = client_provider::get_raw_connected_client(client_provider_config).await?;
    let client = IggyClient::builder().with_client(client).build()?;
    system::login_root(&client).await;
    system::init_by_producer(&args, &client).await?;
    produce_messages(&args, &client).await
}

async fn produce_messages(args: &Args, client: &IggyClient) -> Result<(), Box<dyn Error>> {
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {} ms.",
        args.stream_id, args.topic_id, args.partition_id, args.interval
    );
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(args.interval));
    let mut message_generator = MessagesGenerator::new();
    let mut sent_batches = 0;
    let partitioning = Partitioning::partition_id(args.partition_id);
    loop {
        if args.message_batches_limit > 0 && sent_batches == args.message_batches_limit {
            info!("Sent {sent_batches} batches of messages, exiting.");
            return Ok(());
        }

        interval.tick().await;
        let mut messages = Vec::new();
        let mut serializable_messages = Vec::new();
        for _ in 0..args.messages_per_batch {
            let serializable_message = message_generator.generate();
            // You can send the different message types to the same partition, or stick to the single type.
            let message_type = serializable_message.get_message_type();
            let json = serializable_message.to_json();

            // The message type will be stored in the custom message header.
            let mut headers = HashMap::new();
            headers.insert(
                HeaderKey::new("message_type").unwrap(),
                HeaderValue::from_str(message_type).unwrap(),
            );

            let message = Message::new(None, Bytes::from(json), Some(headers));
            messages.push(message);
            // This is used for the logging purposes only.
            serializable_messages.push(serializable_message);
        }
        client
            .send_messages(
                &args.stream_id.try_into()?,
                &args.topic_id.try_into()?,
                &partitioning,
                &mut messages,
            )
            .await?;
        sent_batches += 1;
        info!("Sent messages: {:#?}", serializable_messages);
    }
}
