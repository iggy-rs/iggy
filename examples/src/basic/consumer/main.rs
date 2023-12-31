use clap::Parser;
use examples::shared::args::Args;
use examples::shared::system;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::models::messages::Message;
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
    let client = client_provider::get_raw_client(client_provider_config).await?;
    let client = client.as_ref();
    system::login_root(client).await;
    system::init_by_consumer(&args, client).await;
    system::consume_messages(&args, client, &handle_message).await
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
