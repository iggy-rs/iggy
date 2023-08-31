use clap::Parser;
use iggy::client::Client;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::consumer::{Consumer, ConsumerKind};
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingStrategy};
use iggy::models::messages::Message;
use samples::shared::args::Args;
use samples::shared::system;
use std::error::Error;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Basic consumer has started, selected transport: {}",
        args.transport
    );
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.to_sdk_args())?);
    let client = client_provider::get_client(client_provider_config).await?;
    let client = client.as_ref();
    system::init_by_consumer(&args, client).await;
    consume_messages(&args, client).await
}

async fn consume_messages(args: &Args, client: &dyn Client) -> Result<(), Box<dyn Error>> {
    info!("Messages will be polled by consumer: {} from stream: {}, topic: {}, partition: {} with interval {} ms.",
        args.consumer_id, args.stream_id, args.topic_id, args.partition_id, args.interval);
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(args.interval));
    loop {
        let polled_messages = client
            .poll_messages(&PollMessages {
                consumer: Consumer {
                    kind: ConsumerKind::from_code(args.consumer_kind)?,
                    id: args.consumer_id,
                },
                stream_id: Identifier::numeric(args.stream_id)?,
                topic_id: Identifier::numeric(args.topic_id)?,
                partition_id: Some(args.partition_id),
                strategy: PollingStrategy::next(),
                count: args.messages_per_batch,
                auto_commit: true,
            })
            .await?;
        if polled_messages.messages.is_empty() {
            info!("No messages found.");
            interval.tick().await;
            continue;
        }
        for message in polled_messages.messages {
            handle_message(&message)?;
        }
        interval.tick().await;
    }
}

fn handle_message(message: &Message) -> Result<(), Box<dyn Error>> {
    // The payload can be of any type as it is a raw byte array. In this case it's a simple string.
    let payload = std::str::from_utf8(&message.payload)?;
    info!(
        "Handling message at offset: {}, payload: {}...",
        message.offset, payload
    );
    Ok(())
}
