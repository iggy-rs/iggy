use anyhow::Result;
use clap::Parser;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::{
    IggyClient, IggyClientBackgroundConfig, PollMessagesConfig, StoreOffsetKind,
};
use iggy::models::messages::PolledMessage;
use iggy_examples::shared::args::Args;
use iggy_examples::shared::messages::*;
use iggy_examples::shared::system;
use std::error::Error;
use std::sync::Arc;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Message envelope consumer has started, selected transport: {}",
        args.transport
    );
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.to_sdk_args())?);
    let client = client_provider::get_raw_connected_client(client_provider_config).await?;
    let client = IggyClient::builder()
        .with_background_config(IggyClientBackgroundConfig {
            poll_messages: PollMessagesConfig {
                interval: args.interval,
                store_offset_kind: StoreOffsetKind::WhenMessagesAreProcessed,
            },
            ..Default::default()
        })
        .with_client(client)
        .build()?;
    system::init_by_consumer(&args, &client).await;
    system::consume_messages(&args, &client, &handle_message).await
}

fn handle_message(message: &PolledMessage) -> Result<(), Box<dyn Error>> {
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
