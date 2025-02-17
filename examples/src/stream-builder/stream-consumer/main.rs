use iggy::client::Client;
use iggy::consumer_ext::IggyConsumerMessageExt;
use iggy::error::IggyError;
use iggy::stream_builder::{IggyConsumerConfig, IggyStreamConsumer};
use iggy_examples::shared::stream::PrintEventConsumer;
use tokio::sync::oneshot;

const IGGY_URL: &str = "iggy://iggy:iggy@localhost:8090";

#[tokio::main]
async fn main() -> Result<(), IggyError> {
    println!("Build iggy client & consumer");
    //For customization, use the `new` or `from_stream_topic` constructor
    let config = IggyConsumerConfig::default();
    let (client, consumer) = IggyStreamConsumer::with_client_from_url(IGGY_URL, &config).await?;

    println!("Start message stream");
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        match consumer
            // PrintEventConsumer is imported from examples/src/shared/stream.rs
            .consume_messages(&PrintEventConsumer {}, rx)
            .await
        {
            Ok(_) => {}
            Err(err) => eprintln!("Failed to consume messages: {err}"),
        }
    });

    // Wait a bit for all messages to arrive.
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    println!("Stop the message stream and shutdown iggy client");
    tx.send(()).expect("Failed to send shutdown signal");
    client.shutdown().await?;
    Ok(())
}
