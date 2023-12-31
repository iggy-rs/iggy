use anyhow::Result;
use clap::Parser;
use examples::shared::args::Args;
use examples::shared::messages::*;
use examples::shared::system;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::{IggyClient, IggyClientConfig, PollMessagesConfig, StoreOffsetKind};
use iggy::models::header::HeaderKey;
use iggy::models::messages::Message;
use std::error::Error;
use std::sync::Arc;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Message headers consumer has started, selected transport: {}",
        args.transport
    );
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.to_sdk_args())?);
    let client = client_provider::get_raw_client(client_provider_config).await?;
    let client = IggyClient::builder(client)
        .with_config(IggyClientConfig {
            poll_messages: PollMessagesConfig {
                interval: args.interval,
                store_offset_kind: StoreOffsetKind::WhenMessagesAreProcessed,
            },
            ..Default::default()
        })
        .build();
    system::login_root(&client).await;
    system::init_by_consumer(&args, &client).await;
    system::consume_messages(&args, &client, &handle_message).await
}

fn handle_message(message: &Message) -> Result<(), Box<dyn Error>> {
    // The payload can be of any type as it is a raw byte array. In this case it's a JSON string.
    let payload = std::str::from_utf8(&message.payload)?;
    // The message type is stored in the custom message header.
    let header_key = HeaderKey::new("message_type").unwrap();
    let message_type = message
        .headers
        .as_ref()
        .unwrap()
        .get(&header_key)
        .unwrap()
        .as_str()?;
    info!(
        "Handling message type: {} at offset: {}...",
        message_type, message.offset
    );
    match message_type {
        ORDER_CREATED_TYPE => {
            let order_created = serde_json::from_str::<OrderCreated>(payload)?;
            info!("{:#?}", order_created);
        }
        ORDER_CONFIRMED_TYPE => {
            let order_confirmed = serde_json::from_str::<OrderConfirmed>(payload)?;
            info!("{:#?}", order_confirmed);
        }
        ORDER_REJECTED_TYPE => {
            let order_rejected = serde_json::from_str::<OrderRejected>(payload)?;
            info!("{:#?}", order_rejected);
        }
        _ => {
            warn!("Received unknown message type: {}", message_type);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use examples::shared::utils;
    use iggy::models::header::HeaderValue;
    use iggy::models::messages::MessageState;
    use std::collections::HashMap;
    use std::str::FromStr;
    use tracing_test::traced_test;

    fn create_headers(message_type: &str) -> HashMap<HeaderKey, HeaderValue> {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("Message_type").unwrap(),
            HeaderValue::from_str(message_type).unwrap(),
        );
        headers
    }

    #[test]
    #[traced_test]
    fn handle_message_should_succeed_for_valid_order_created_type() {
        let test_order_created = OrderCreated {
            order_id: 1,
            currency_pair: "EUR/USD".to_string(),
            price: 3.45,
            quantity: 1.0,
            side: "buy".to_string(),
            timestamp: utils::timestamp(),
        }
        .to_json();
        let message = Message::empty(
            0,
            MessageState::Available,
            0,
            Bytes::from(test_order_created),
            0,
            Some(create_headers("order_created")),
        );
        let res = handle_message(&message);
        assert!(res.is_ok());
        assert!(logs_contain(
            "Handling message type: order_created at offset: 0..."
        ));
        assert!(logs_contain("OrderCreated"));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "INFO", 2));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "WARN", 0));
    }

    #[test]
    #[traced_test]
    fn handle_message_should_succeed_for_valid_order_confirmed_type() {
        let test_order_confirmed = OrderConfirmed {
            order_id: 1,
            price: 3.45,
            timestamp: utils::timestamp(),
        }
        .to_json();
        let message = Message::empty(
            0,
            MessageState::Available,
            0,
            Bytes::from(test_order_confirmed),
            0,
            Some(create_headers("order_confirmed")),
        );
        let res = handle_message(&message);
        assert!(res.is_ok());
        assert!(logs_contain(
            "Handling message type: order_confirmed at offset: 0..."
        ));
        assert!(logs_contain("OrderConfirmed"));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "INFO", 2));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "WARN", 0));
    }

    #[test]
    #[traced_test]
    fn handle_message_should_succeed_for_valid_order_rejected_type() {
        let test_order_rejected = OrderRejected {
            order_id: 1,
            timestamp: utils::timestamp(),
            reason: "cancelled_by_user".to_string(),
        }
        .to_json();
        let message = Message::empty(
            0,
            MessageState::Available,
            0,
            Bytes::from(test_order_rejected),
            0,
            Some(create_headers("order_rejected")),
        );
        let res = handle_message(&message);
        assert!(res.is_ok());
        assert!(logs_contain(
            "Handling message type: order_rejected at offset: 0..."
        ));
        assert!(logs_contain("OrderRejected"));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "INFO", 2));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "WARN", 0));
    }

    #[test]
    #[traced_test]
    fn handle_message_should_warn_for_invalid_order_type() {
        let message = Message::empty(
            0,
            MessageState::Available,
            0,
            Bytes::from(""),
            0,
            Some(create_headers("order_unknown")),
        );
        let res = handle_message(&message);
        assert!(res.is_ok());
        assert!(logs_contain(
            "Handling message type: order_unknown at offset: 0..."
        ));
        assert!(logs_contain("Received unknown message type: order_unknown"));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "INFO", 1));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "WARN", 1));
    }

    #[test]
    #[traced_test]
    fn handle_message_should_report_error_for_valid_order_type_with_invalid_payload() {
        let message = Message::empty(
            0,
            MessageState::Available,
            0,
            Bytes::from(""),
            0,
            Some(create_headers("order_created")),
        );
        let res = handle_message(&message);
        assert!(res.is_err());
        assert!(logs_contain(
            "Handling message type: order_created at offset: 0..."
        ));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "INFO", 1));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "WARN", 0));
    }

    #[test]
    #[traced_test]
    #[should_panic]
    #[allow(unused_must_use)]
    fn handle_message_should_panic_for_message_with_missing_headers() {
        let message = Message::empty(0, MessageState::Available, 0, Bytes::from(""), 0, None);
        handle_message(&message); // panicked: called `Option::unwrap()` on a `None` value
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "INFO", 0));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "WARN", 0));
    }
}
