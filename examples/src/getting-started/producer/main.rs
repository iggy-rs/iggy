use iggy::client::{Client, StreamClient, TopicClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy::streams::create_stream::CreateStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::users::defaults::*;
use iggy::users::login_user::LoginUser;
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

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
    init_system(&client).await;
    produce_messages(&client).await
}

async fn init_system(client: &dyn Client) {
    match client
        .create_stream(&CreateStream {
            stream_id: STREAM_ID,
            name: "sample-stream".to_string(),
        })
        .await
    {
        Ok(_) => info!("Stream was created."),
        Err(_) => warn!("Stream already exists and will not be created again."),
    }

    match client
        .create_topic(&CreateTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: TOPIC_ID,
            partitions_count: 1,
            name: "sample-topic".to_string(),
            message_expiry: None,
        })
        .await
    {
        Ok(_) => info!("Topic was created."),
        Err(_) => warn!("Topic already exists and will not be created again."),
    }
}

async fn produce_messages(client: &dyn Client) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {} ms.",
        STREAM_ID,
        TOPIC_ID,
        PARTITION_ID,
        interval.as_millis()
    );

    let mut current_id = 0;
    let messages_per_batch = 10;
    let mut sent_batches = 0;
    loop {
        if sent_batches == BATCHES_LIMIT {
            info!("Sent {sent_batches} batches of messages, exiting.");
            return Ok(());
        }

        let mut messages = Vec::new();
        for _ in 0..messages_per_batch {
            current_id += 1;
            let payload = format!("message-{current_id}");
            let message = Message::from_str(&payload)?;
            messages.push(message);
        }
        client
            .send_messages(&mut SendMessages {
                stream_id: Identifier::numeric(STREAM_ID)?,
                topic_id: Identifier::numeric(TOPIC_ID)?,
                partitioning: Partitioning::partition_id(PARTITION_ID),
                messages,
            })
            .await?;
        sent_batches += 1;
        info!("Sent {messages_per_batch} message(s).");
        sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use examples::shared::mock_client::*;
    use examples::shared::utils;
    use iggy::error::Error;
    use iggy::topics::create_topic::CreateTopic;
    use mockall::predicate;
    use tracing_test::traced_test;

    static CREATE_STREAM_SUCCESS_OUTPUT: &str = "Stream was created.";
    static CREATE_TOPIC_SUCCESS_OUTPUT: &str = "Topic was created.";
    static CREATE_STREAM_ERROR_OUTPUT: &str =
        "Stream already exists and will not be created again.";
    static CREATE_TOPIC_ERROR_OUTPUT: &str = "Topic already exists and will not be created again.";

    fn setup_mock_client(existing_stream: bool, existing_topic: bool) -> MockIggyClient {
        let mut mock_client = MockIggyClient::new();
        let expected_create_stream = CreateStream {
            stream_id: STREAM_ID,
            name: "sample-stream".to_string(),
        };
        let expected_create_topic: CreateTopic = CreateTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: TOPIC_ID,
            partitions_count: 1,
            name: "sample-topic".to_string(),
            message_expiry: None,
        };
        mock_client
            .expect_create_stream()
            .with(predicate::eq(expected_create_stream)) // Use predicate::eq() to compare references
            .times(1)
            .returning(move |_| {
                if !existing_stream {
                    Ok(())
                } else {
                    Err(Error::Error)
                }
            });
        mock_client
            .expect_create_topic()
            .with(predicate::eq(expected_create_topic)) // Use predicate::eq() to compare references
            .times(1)
            .returning(move |_| {
                if !existing_topic {
                    Ok(())
                } else {
                    Err(Error::Error)
                }
            });
        return mock_client;
    }

    #[tokio::test]
    #[traced_test]
    async fn should_init_system_with_no_existing_stream_or_topic() {
        let mock_client = setup_mock_client(false, false);
        init_system(&mock_client).await;
        assert!(logs_contain(CREATE_STREAM_SUCCESS_OUTPUT));
        assert!(logs_contain(CREATE_TOPIC_SUCCESS_OUTPUT));
        assert!(!logs_contain(CREATE_STREAM_ERROR_OUTPUT));
        assert!(!logs_contain(CREATE_TOPIC_ERROR_OUTPUT));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "INFO", 2));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "WARN", 0));
    }

    #[tokio::test]
    #[traced_test]
    async fn should_init_system_with_existing_stream_and_topic() {
        let mock_client = setup_mock_client(true, true);
        init_system(&mock_client).await;

        assert!(logs_contain(CREATE_STREAM_ERROR_OUTPUT));
        assert!(logs_contain(CREATE_TOPIC_ERROR_OUTPUT));
        assert!(!logs_contain(CREATE_STREAM_SUCCESS_OUTPUT));
        assert!(!logs_contain(CREATE_TOPIC_SUCCESS_OUTPUT));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "INFO", 0));
        logs_assert(|lines: &[&str]| utils::matching_log_entry_counts(lines, "WARN", 2));
    }
}
