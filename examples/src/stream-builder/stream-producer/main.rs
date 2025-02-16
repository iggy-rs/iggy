use iggy::client::{Client, StreamClient};
use iggy::error::IggyError;
use iggy::messages::send_messages::Message;
use iggy::stream_builder::{IggyProducerConfig, IggyStreamProducer};
use std::str::FromStr;

const IGGY_URL: &str = "iggy://iggy:iggy@localhost:8090";

#[tokio::main]
async fn main() -> Result<(), IggyError> {
    println!("Build iggy client and producer");
    //For customization, use the `new` or `from_stream_topic` constructor
    let config = IggyProducerConfig::default();
    let (client, producer) = IggyStreamProducer::with_client_from_url(IGGY_URL, &config).await?;

    println!("Send 3 test messages...");
    producer.send_one(Message::from_str("Hello World")?).await?;
    producer.send_one(Message::from_str("Hola Iggy")?).await?;
    producer.send_one(Message::from_str("Hi Apache")?).await?;

    // Wait a bit for all messages to arrive.
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    println!("Stop the message stream and shutdown iggy client");
    client.delete_stream(config.stream_id()).await?;
    client.shutdown().await?;
    Ok(())
}
