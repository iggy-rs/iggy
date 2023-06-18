use anyhow::Result;
use clap::Parser;
use rand::Rng;
use samples::shared::args::Args;
use samples::shared::messages::OrderCreated;
use samples::shared::utils;
use sdk::client::Client;
use sdk::client_provider;
use sdk::client_provider::ClientProviderConfig;
use sdk::messages::send_messages::{KeyKind, Message, SendMessages};
use sdk::streams::create_stream::CreateStream;
use sdk::streams::get_stream::GetStream;
use sdk::topics::create_topic::CreateTopic;
use std::error::Error;
use std::str::FromStr;
use tracing::info;

const CURRENCY_PAIRS: &[&str] = &["EUR/USD", "EUR/GBP", "USD/GBP", "EUR/PLN", "USD/PLN"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Producer has started, selected transport: {}",
        args.transport
    );
    let client_provider_config = ClientProviderConfig::from_args(args.to_sdk_args())?;
    let client = client_provider::get_client(client_provider_config).await?;
    let client = client.as_ref();
    let stream_id = 9999;
    let topic_id = 1;
    let partition_id = 1;
    let stream = client.get_stream(&GetStream { stream_id }).await;
    if stream.is_err() {
        info!("Stream does not exist, creating...");
        client
            .create_stream(&CreateStream {
                stream_id,
                name: "sample".to_string(),
            })
            .await?;
        client
            .create_topic(&CreateTopic {
                stream_id,
                topic_id,
                partitions_count: 1,
                name: "orders".to_string(),
            })
            .await?;
    }

    produce_messages(stream_id, topic_id, partition_id, args.interval, client).await
}

pub async fn produce_messages(
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
    interval: u64,
    client: &dyn Client,
) -> Result<(), Box<dyn Error>> {
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {} ms.",
        stream_id, topic_id, partition_id, interval
    );
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval));
    let mut order_id = 1;
    let mut rng = rand::thread_rng();
    loop {
        let price = rng.gen_range(10.0..=1000.0);
        let quantity = rng.gen_range(0.1..=1.0);
        let currency_pair = CURRENCY_PAIRS[rng.gen_range(0..CURRENCY_PAIRS.len())].to_string();
        let side = match rng.gen_range(0..=1) {
            0 => "buy",
            _ => "sell",
        }
        .to_string();
        let timestamp = utils::timestamp();
        let order_created = OrderCreated {
            id: order_id,
            currency_pair,
            price,
            quantity,
            side,
            timestamp,
        };
        let json_envelope = order_created.to_json_envelope();
        let message = Message::from_str(&json_envelope)?;
        let messages = vec![message];
        client
            .send_messages(&SendMessages {
                stream_id,
                topic_id,
                key_kind: KeyKind::PartitionId,
                key_value: partition_id,
                messages_count: messages.len() as u32,
                messages,
            })
            .await?;
        order_id += 1;
        info!("Sent message: {:?}", order_created);
        interval.tick().await;
    }
}
