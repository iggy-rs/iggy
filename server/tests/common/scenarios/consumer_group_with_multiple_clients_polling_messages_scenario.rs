use crate::common::{ClientFactory, TestServer};
use iggy::client::{ConsumerGroupClient, MessageClient, StreamClient, TopicClient};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::consumer_type::ConsumerType;
use iggy::messages::poll_messages::Kind::Next;
use iggy::messages::poll_messages::{Format, PollMessages};
use iggy::messages::send_messages::{Key, Message, SendMessages};
use iggy::streams::create_stream::CreateStream;
use iggy::topics::create_topic::CreateTopic;
use std::str::FromStr;
use tokio::time::sleep;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const CONSUMER_GROUP_ID: u32 = 1;
const MESSAGES_COUNT: u32 = 1000;

#[allow(dead_code)]
pub async fn run(client_factory: &dyn ClientFactory) {
    let test_server = TestServer::default();
    test_server.start();
    sleep(std::time::Duration::from_secs(1)).await;
    let system_client = create_client(client_factory).await;
    let client1 = create_client(client_factory).await;
    let client2 = create_client(client_factory).await;
    let client3 = create_client(client_factory).await;

    // 1. Create the stream
    let create_stream = CreateStream {
        stream_id: STREAM_ID,
        name: STREAM_NAME.to_string(),
    };
    system_client.create_stream(&create_stream).await.unwrap();

    // 2. Create the topic
    let create_topic = CreateTopic {
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        partitions_count: PARTITIONS_COUNT,
        name: TOPIC_NAME.to_string(),
    };
    system_client.create_topic(&create_topic).await.unwrap();

    // 3. Create the consumer group
    let create_group = CreateConsumerGroup {
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        consumer_group_id: CONSUMER_GROUP_ID,
    };
    system_client
        .create_consumer_group(&create_group)
        .await
        .unwrap();

    let join_group = JoinConsumerGroup {
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        consumer_group_id: CONSUMER_GROUP_ID,
    };

    // 4. Join the consumer group by each client
    client1.join_consumer_group(&join_group).await.unwrap();
    client2.join_consumer_group(&join_group).await.unwrap();
    client3.join_consumer_group(&join_group).await.unwrap();

    // 5. Send messages to the calculated partition ID on the server side by using entity ID as a key
    for entity_id in 1..=MESSAGES_COUNT {
        let message = Message::from_str(&get_message_payload(entity_id)).unwrap();
        let messages = vec![message];
        let send_messages = SendMessages {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            key: Key::entity_id_u32(entity_id),
            messages_count: 1,
            messages,
        };
        system_client.send_messages(&send_messages).await.unwrap();
    }

    // 6. Get the consumer group details
    let consumer_group_info = system_client
        .get_consumer_group(&GetConsumerGroup {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            consumer_group_id: CONSUMER_GROUP_ID,
        })
        .await
        .unwrap();

    for member in &consumer_group_info.members {
        assert_eq!(member.partitions.len(), 1);
    }

    // 7. Poll the messages for each client per assigned partition in the consumer group
    let mut total_read_messages_count = 0;
    total_read_messages_count += poll_messages(&client1).await;
    total_read_messages_count += poll_messages(&client2).await;
    total_read_messages_count += poll_messages(&client3).await;

    assert_eq!(total_read_messages_count, MESSAGES_COUNT);

    test_server.stop();
}

async fn poll_messages(client: &IggyClient) -> u32 {
    let poll_messages = PollMessages {
        consumer_type: ConsumerType::ConsumerGroup,
        consumer_id: CONSUMER_GROUP_ID,
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        partition_id: 0,
        kind: Next,
        value: 0,
        count: 1,
        auto_commit: true,
        format: Format::None,
    };

    let mut total_read_messages_count = 0;
    for _ in 1..=PARTITIONS_COUNT * MESSAGES_COUNT {
        let messages = client.poll_messages(&poll_messages).await.unwrap();
        total_read_messages_count += messages.len() as u32;
    }

    total_read_messages_count
}

fn get_message_payload(entity_id: u32) -> String {
    format!("message-{}", entity_id)
}

async fn create_client(client_factory: &dyn ClientFactory) -> IggyClient {
    let client = client_factory.create_client().await;
    IggyClient::new(client, IggyClientConfig::default())
}
