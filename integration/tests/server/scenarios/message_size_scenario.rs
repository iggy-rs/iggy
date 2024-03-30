use bytes::Bytes;
use iggy::client::{MessageClient, StreamClient, TopicClient};
use iggy::clients::client::{IggyClient, IggyClientBackgroundConfig};
use iggy::consumer::Consumer;
use iggy::error::IggyError;
use iggy::error::IggyError::InvalidResponse;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingStrategy};
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::topics::create_topic::CreateTopic;
use integration::test_server::{assert_clean_system, login_root, ClientFactory};
use std::collections::HashMap;
use std::str::FromStr;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const PARTITION_ID: u32 = 1;
const MAX_HEADER_SIZE: usize = 255;

enum MessageToSend {
    NoMessage,
    OfSize(usize),
    OfSizeWithHeaders(usize, usize),
}

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = client_factory.create_client().await;
    let client = IggyClient::create(
        client,
        IggyClientBackgroundConfig::default(),
        None,
        None,
        None,
    );

    login_root(&client).await;
    init_system(&client).await;

    // 3. Send message and check the result
    send_message_and_check_result(
        &client,
        MessageToSend::NoMessage,
        Err(IggyError::InvalidMessagesCount),
    )
    .await;
    send_message_and_check_result(&client, MessageToSend::OfSize(1), Ok(())).await;
    send_message_and_check_result(&client, MessageToSend::OfSize(1_000_000), Ok(())).await;
    send_message_and_check_result(&client, MessageToSend::OfSize(10_000_000), Ok(())).await;
    send_message_and_check_result(
        &client,
        MessageToSend::OfSizeWithHeaders(1, 10_000_000),
        Ok(()),
    )
    .await;
    send_message_and_check_result(
        &client,
        MessageToSend::OfSizeWithHeaders(1_000, 10_000_000),
        Ok(()),
    )
    .await;
    send_message_and_check_result(
        &client,
        MessageToSend::OfSizeWithHeaders(100_000, 10_000_000),
        Ok(()),
    )
    .await;
    send_message_and_check_result(
        &client,
        MessageToSend::OfSizeWithHeaders(100_001, 10_000_000),
        Err(InvalidResponse(
            4017,
            23,
            "Too big headers payload".to_owned(),
        )),
    )
    .await;
    send_message_and_check_result(
        &client,
        MessageToSend::OfSizeWithHeaders(100_000, 10_000_001),
        Err(InvalidResponse(
            4022,
            23,
            "Too big message payload".to_owned(),
        )),
    )
    .await;

    assert_message_count(&client, 6).await;
    cleanup_system(&client).await;
    assert_clean_system(&client).await;
}

async fn assert_message_count(client: &IggyClient, expected_count: u32) {
    // 4. Poll messages and validate the headers
    let poll_messages = PollMessages {
        consumer: Consumer::default(),
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        partition_id: Some(PARTITION_ID),
        strategy: PollingStrategy::offset(0),
        count: expected_count * 2,
        auto_commit: false,
    };

    let polled_messages = client.poll_messages(&poll_messages).await.unwrap();
    assert_eq!(polled_messages.messages.len() as u32, expected_count);
}

async fn init_system(client: &IggyClient) {
    // 1. Create the stream
    let create_stream = CreateStream {
        stream_id: Some(STREAM_ID),
        name: STREAM_NAME.to_string(),
    };
    client.create_stream(&create_stream).await.unwrap();

    // 2. Create the topic
    let create_topic = CreateTopic {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Some(TOPIC_ID),
        partitions_count: PARTITIONS_COUNT,
        compression_algorithm: Default::default(),
        name: TOPIC_NAME.to_string(),
        message_expiry: None,
        max_topic_size: None,
        replication_factor: 1,
    };
    client.create_topic(&create_topic).await.unwrap();
}

async fn cleanup_system(client: &IggyClient) {
    let delete_stream = DeleteStream {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
    };
    client.delete_stream(&delete_stream).await.unwrap();
}

async fn send_message_and_check_result(
    client: &IggyClient,
    message_params: MessageToSend,
    expected_result: Result<(), IggyError>,
) {
    let mut messages = Vec::new();

    match message_params {
        MessageToSend::NoMessage => {
            println!("Sending message without messages inside");
        }
        MessageToSend::OfSize(size) => {
            println!("Sending message with payload size = {size}");
            messages.push(create_message(None, size));
        }
        MessageToSend::OfSizeWithHeaders(header_size, payload_size) => {
            println!("Sending message with header size = {header_size} and payload size = {payload_size}");
            messages.push(create_message(Some(header_size), payload_size));
        }
    };

    let mut send_messages = SendMessages {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        partitioning: Partitioning::partition_id(PARTITION_ID),
        messages,
    };

    let send_result = client.send_messages(&mut send_messages).await;
    println!("Received = {send_result:?}, expected = {expected_result:?}");
    match expected_result {
        Ok(()) => assert!(send_result.is_ok()),
        Err(error) => {
            assert!(send_result.is_err());
            let send_result = send_result.err().unwrap();
            assert_eq!(error.as_code(), send_result.as_code());
            assert_eq!(error.to_string(), send_result.to_string());
        }
    }
}

fn create_string_of_size(size: usize) -> String {
    "x".repeat(size)
}

fn create_message_header_of_size(size: usize) -> HashMap<HeaderKey, HeaderValue> {
    let mut headers = HashMap::new();
    let mut header_id = 1;
    let mut size = size;

    while size > 0 {
        let header_size = if size > MAX_HEADER_SIZE {
            MAX_HEADER_SIZE
        } else {
            size
        };
        headers.insert(
            HeaderKey::new(format!("header-{header_id}").as_str()).unwrap(),
            HeaderValue::from_str(create_string_of_size(header_size).as_str()).unwrap(),
        );
        header_id += 1;
        size -= header_size;
    }

    headers
}

fn create_message(header_size: Option<usize>, payload_size: usize) -> Message {
    let headers = match header_size {
        Some(header_size) => {
            if header_size > 0 {
                Some(create_message_header_of_size(header_size))
            } else {
                None
            }
        }
        None => None,
    };

    let payload = create_string_of_size(payload_size);
    Message {
        id: 1u128,
        length: payload.len() as u32,
        payload: Bytes::from(payload),
        headers,
    }
}
