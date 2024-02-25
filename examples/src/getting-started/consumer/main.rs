use iggy::client::{Client, UserClient};
use iggy::clients::client::IggyClientBuilder;
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingStrategy};
use iggy::models::messages::PolledMessage;
use iggy::users::defaults::*;
use iggy::users::login_user::LoginUser;
use std::env;
use std::error::Error;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;
const BATCHES_LIMIT: u32 = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(get_tcp_server_addr())
        .build()?;

    // Or, instead of above lines, you can just use below code, which will create a Iggy
    // TCP client with default config (default server address for TCP is 127.0.0.1:8090):
    // let client = IggyClient::default();

    client.connect().await?;
    client
        .login_user(&LoginUser {
            username: DEFAULT_ROOT_USERNAME.to_string(),
            password: DEFAULT_ROOT_PASSWORD.to_string(),
        })
        .await?;

    consume_messages(&client).await
}

async fn consume_messages(client: &dyn Client) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    info!(
        "Messages will be consumed from stream: {}, topic: {}, partition: {} with interval {} ms.",
        STREAM_ID,
        TOPIC_ID,
        PARTITION_ID,
        interval.as_millis()
    );

    let mut offset = 0;
    let messages_per_batch = 10;
    let mut consumed_batches = 0;
    loop {
        if consumed_batches == BATCHES_LIMIT {
            info!("Consumed {consumed_batches} batches of messages, exiting.");
            return Ok(());
        }

        let polled_messages = client
            .poll_messages(&PollMessages {
                consumer: Consumer::default(),
                stream_id: Identifier::numeric(STREAM_ID)?,
                topic_id: Identifier::numeric(TOPIC_ID)?,
                partition_id: Some(PARTITION_ID),
                strategy: PollingStrategy::offset(offset),
                count: messages_per_batch,
                auto_commit: false,
            })
            .await?;
        if polled_messages.messages.is_empty() {
            info!("No messages found.");
            sleep(interval).await;
            continue;
        }

        offset += polled_messages.messages.len() as u64;
        for message in polled_messages.messages {
            handle_message(&message)?;
        }
        consumed_batches += 1;
        sleep(interval).await;
    }
}

fn handle_message(message: &PolledMessage) -> Result<(), Box<dyn Error>> {
    // The payload can be of any type as it is a raw byte array. In this case it's a simple string.
    let payload = std::str::from_utf8(&message.payload)?;
    info!(
        "Handling message at offset: {}, payload: {}...",
        message.offset, payload
    );
    Ok(())
}

fn get_tcp_server_addr() -> String {
    let default_server_addr = "127.0.0.1:8090".to_string();
    let argument_name = env::args().nth(1);
    let tcp_server_addr = env::args().nth(2);

    if argument_name.is_none() && tcp_server_addr.is_none() {
        default_server_addr
    } else {
        let argument_name = argument_name.unwrap();
        if argument_name != "--tcp-server-address" {
            panic!(
                "Invalid argument {}! Usage: {} --tcp-server-address <server-address>",
                argument_name,
                env::args().next().unwrap()
            );
        }
        let tcp_server_addr = tcp_server_addr.unwrap();
        if tcp_server_addr.parse::<std::net::SocketAddr>().is_err() {
            panic!(
                "Invalid server address {}! Usage: {} --tcp-server-address <server-address>",
                tcp_server_addr,
                env::args().next().unwrap()
            );
        }
        info!("Using server address: {}", tcp_server_addr);
        tcp_server_addr
    }
}
