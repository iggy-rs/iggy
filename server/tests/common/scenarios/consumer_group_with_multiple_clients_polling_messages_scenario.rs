use crate::common::{ClientFactory, TestServer};
use sdk::client::{ConsumerGroupClient, MessageClient, StreamClient, SystemClient, TopicClient};
use sdk::clients::client::{IggyClient, IggyClientConfig};
use sdk::consumer_groups::create_consumer_group::CreateConsumerGroup;
use sdk::consumer_groups::get_consumer_group::GetConsumerGroup;
use sdk::consumer_groups::join_consumer_group::JoinConsumerGroup;
use sdk::consumer_type::ConsumerType;
use sdk::messages::poll_messages::Kind::Next;
use sdk::messages::poll_messages::{Format, PollMessages};
use sdk::messages::send_messages::{KeyKind, Message, SendMessages};
use sdk::models::consumer_group::ConsumerGroupDetails;
use sdk::streams::create_stream::CreateStream;
use sdk::system::get_me::GetMe;
use sdk::topics::create_topic::CreateTopic;
use std::str::{from_utf8, FromStr};
use tokio::time::sleep;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const CONSUMER_GROUP_ID: u32 = 1;
const MESSAGES_COUNT_PER_PARTITION: u32 = 10;

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
    for entity_id in 1..=MESSAGES_COUNT_PER_PARTITION * PARTITIONS_COUNT {
        let mut partition_id = entity_id % PARTITIONS_COUNT;
        if partition_id == 0 {
            partition_id = PARTITIONS_COUNT;
        }

        let message = Message::from_str(&get_message_payload(partition_id, entity_id)).unwrap();
        let messages = vec![message];
        let send_messages = SendMessages {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            key_kind: KeyKind::EntityId,
            key_value: entity_id,
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
    validate_message_polling(&client1, &consumer_group_info).await;
    validate_message_polling(&client2, &consumer_group_info).await;
    validate_message_polling(&client3, &consumer_group_info).await;

    test_server.stop();
}

async fn validate_message_polling(client: &IggyClient, consumer_group: &ConsumerGroupDetails) {
    let client_info = client.get_me(&GetMe {}).await.unwrap();
    let consumer_group_member = consumer_group
        .members
        .iter()
        .find(|m| m.id == client_info.id)
        .unwrap();
    let partition_id = consumer_group_member.partitions[0];
    let mut start_entity_id = partition_id % PARTITIONS_COUNT;
    if start_entity_id == 0 {
        start_entity_id = PARTITIONS_COUNT;
    }

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

    for i in 1..=MESSAGES_COUNT_PER_PARTITION {
        let messages = client.poll_messages(&poll_messages).await.unwrap();
        assert_eq!(messages.len(), 1);
        let message = &messages[0];
        let offset = (i - 1) as u64;
        assert_eq!(message.offset, offset);
        let entity_id = start_entity_id + ((i - 1) * PARTITIONS_COUNT);
        let payload = from_utf8(&message.payload).unwrap();
        assert_eq!(payload, &get_message_payload(partition_id, entity_id));
    }

    let messages = client.poll_messages(&poll_messages).await.unwrap();
    assert!(messages.is_empty())
}

fn get_message_payload(partition_id: u32, entity_id: u32) -> String {
    format!("message-{}-{}", partition_id, entity_id)
}

async fn create_client(client_factory: &dyn ClientFactory) -> IggyClient {
    let client = client_factory.create_client().await;
    IggyClient::new(client, IggyClientConfig::default())
}
