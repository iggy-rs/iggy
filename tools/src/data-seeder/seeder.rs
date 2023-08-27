use iggy::client::{MessageClient, StreamClient, TopicClient};
use iggy::clients::client::IggyClient;
use iggy::error::Error;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy::streams::create_stream::CreateStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::get_topics::GetTopics;
use rand::Rng;
use std::collections::HashMap;
use std::str::FromStr;

const PROD_STREAM_ID: u32 = 1;
const TEST_STREAM_ID: u32 = 2;
const DEV_STREAM_ID: u32 = 3;

pub async fn seed(client: &IggyClient) -> Result<(), Error> {
    create_streams(client).await?;
    create_topics(client).await?;
    send_messages(client).await?;
    Ok(())
}

async fn create_streams(client: &IggyClient) -> Result<(), Error> {
    client
        .create_stream(&CreateStream {
            stream_id: PROD_STREAM_ID,
            name: "prod".to_string(),
        })
        .await?;
    client
        .create_stream(&CreateStream {
            stream_id: TEST_STREAM_ID,
            name: "test".to_string(),
        })
        .await?;
    client
        .create_stream(&CreateStream {
            stream_id: DEV_STREAM_ID,
            name: "dev".to_string(),
        })
        .await?;
    Ok(())
}

async fn create_topics(client: &IggyClient) -> Result<(), Error> {
    let streams = [PROD_STREAM_ID, TEST_STREAM_ID, DEV_STREAM_ID];
    for stream_id in streams {
        client
            .create_topic(&CreateTopic {
                stream_id: Identifier::numeric(stream_id)?,
                topic_id: 1,
                name: "orders".to_string(),
                partitions_count: 1,
                message_expiry: None,
            })
            .await?;

        client
            .create_topic(&CreateTopic {
                stream_id: Identifier::numeric(stream_id)?,
                topic_id: 2,
                name: "users".to_string(),
                partitions_count: 2,
                message_expiry: None,
            })
            .await?;

        client
            .create_topic(&CreateTopic {
                stream_id: Identifier::numeric(stream_id)?,
                topic_id: 3,
                name: "notifications".to_string(),
                partitions_count: 3,
                message_expiry: None,
            })
            .await?;

        client
            .create_topic(&CreateTopic {
                stream_id: Identifier::numeric(stream_id)?,
                topic_id: 4,
                name: "payments".to_string(),
                partitions_count: 2,
                message_expiry: None,
            })
            .await?;

        client
            .create_topic(&CreateTopic {
                stream_id: Identifier::numeric(stream_id)?,
                topic_id: 5,
                name: "deliveries".to_string(),
                partitions_count: 1,
                message_expiry: None,
            })
            .await?;
    }
    Ok(())
}

async fn send_messages(client: &IggyClient) -> Result<(), Error> {
    let mut rng = rand::thread_rng();
    let streams = [PROD_STREAM_ID, TEST_STREAM_ID, DEV_STREAM_ID];
    for stream_id in streams {
        let topics = client
            .get_topics(&GetTopics {
                stream_id: Identifier::numeric(stream_id)?,
            })
            .await?;

        for topic in topics {
            let mut messages = Vec::new();
            let message_batches = rng.gen_range(100..=1000);
            let mut message_id = 1;
            for _ in 1..=message_batches {
                let messages_count = rng.gen_range(10..=100);
                for _ in 1..=messages_count {
                    let payload = format!("{}_data_{}", topic.name, message_id);
                    let headers = match rng.gen_bool(0.5) {
                        false => None,
                        true => {
                            let mut headers = HashMap::new();
                            headers
                                .insert(HeaderKey::new("key 1")?, HeaderValue::from_str("value1")?);
                            headers.insert(HeaderKey::new("key-2")?, HeaderValue::from_bool(true)?);
                            headers.insert(
                                HeaderKey::new("key_3")?,
                                HeaderValue::from_uint64(123456)?,
                            );
                            Some(headers)
                        }
                    };
                    let mut message = Message::from_str(&payload)?;
                    message.headers = headers;
                    messages.push(message);
                    message_id += 1;
                }
                client
                    .send_messages(&mut SendMessages {
                        stream_id: Identifier::numeric(stream_id)?,
                        topic_id: Identifier::numeric(topic.id)?,
                        partitioning: Partitioning::balanced(),
                        messages,
                    })
                    .await?;
                messages = Vec::new();
            }
        }
    }
    Ok(())
}
