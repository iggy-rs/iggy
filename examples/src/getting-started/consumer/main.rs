use iggy::client::{Client, UserClient};
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingStrategy};
use iggy::models::messages::Message;
use iggy::users::defaults::*;
use iggy::users::login_user::LoginUser;
use std::error::Error;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;
const BATCHES_LIMIT: u32 = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let client = IggyClient::default();
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

fn handle_message(message: &Message) -> Result<(), Box<dyn Error>> {
    // The payload can be of any type as it is a raw byte array. In this case it's a simple string.
    let payload = std::str::from_utf8(&message.payload)?;
    info!(
        "Handling message at offset: {}, payload: {}...",
        message.offset, payload
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use examples::shared::utils;
    use iggy::models::messages::MessageState;
    use tracing_test::traced_test;

    static HANDLE_MESSAGE_INFO_OUTPUT: &str =
        "Handling message at offset: 0, payload: message-1...";

    #[test]
    #[traced_test]
    fn handle_message_should_succeed_with_valid_payload() {
        let message = Message::empty(
            0,
            MessageState::Available,
            0,
            Bytes::from("message-1"),
            0,
            None,
        );
        let result = handle_message(&message);
        assert!(result.is_ok());
        assert!(logs_contain(HANDLE_MESSAGE_INFO_OUTPUT));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "INFO", 1));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "WARN", 0));
    }

    #[test]
    #[traced_test]
    fn handle_message_should_report_error_with_invalid_payload() {
        let invalid_utf8_bytes = Bytes::from_static(b"\xC3\x28"); // Invalid UTF-8 sequence
        let message = Message::empty(0, MessageState::Available, 0, invalid_utf8_bytes, 0, None);
        let result = handle_message(&message);
        assert!(result.is_err());
        assert!(!logs_contain(HANDLE_MESSAGE_INFO_OUTPUT));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "INFO", 0));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "WARN", 0));
    }
}
