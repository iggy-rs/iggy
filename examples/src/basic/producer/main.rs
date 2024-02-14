use clap::Parser;
use iggy::client::Client;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy_examples::shared::args::Args;
use iggy_examples::shared::system;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Basic producer has started, selected transport: {}",
        args.transport
    );
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.to_sdk_args())?);
    let client = client_provider::get_raw_connected_client(client_provider_config).await?;
    let client = client.as_ref();
    system::login_root(client).await;
    system::init_by_producer(&args, client).await?;
    produce_messages(&args, client).await
}

async fn produce_messages(args: &Args, client: &dyn Client) -> Result<(), Box<dyn Error>> {
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {} ms.",
        args.stream_id, args.topic_id, args.partition_id, args.interval
    );
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(args.interval));
    let mut current_id = 0u64;
    let mut sent_batches = 0;
    loop {
        if args.message_batches_limit > 0 && sent_batches == args.message_batches_limit {
            info!("Sent {sent_batches} batches of messages, exiting.");
            return Ok(());
        }

        let mut messages = Vec::new();
        let mut sent_messages = Vec::new();
        for _ in 0..args.messages_per_batch {
            current_id += 1;
            let payload = format!("message-{current_id}");
            let message = Message::from_str(&payload)?;
            messages.push(message);
            sent_messages.push(payload);
        }
        client
            .send_messages(&mut SendMessages {
                stream_id: Identifier::numeric(args.stream_id)?,
                topic_id: Identifier::numeric(args.topic_id)?,
                partitioning: Partitioning::partition_id(args.partition_id),
                messages,
            })
            .await?;
        sent_batches += 1;
        info!("Sent messages: {:#?}", sent_messages);
        interval.tick().await;
    }
}
