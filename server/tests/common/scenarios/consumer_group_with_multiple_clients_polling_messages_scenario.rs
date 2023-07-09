use crate::common::{ClientFactory, TestServer};
use sdk::client::Client;
use sdk::consumer_type::ConsumerType;
use sdk::groups::create_group::CreateGroup;
use sdk::groups::get_group::GetGroup;
use sdk::groups::join_group::JoinGroup;
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
const GROUP_ID: u32 = 1;
const MESSAGES_COUNT_PER_PARTITION: u32 = 10;

#[allow(dead_code)]
pub async fn run(client_factory: &dyn ClientFactory) {
    let test_server = TestServer::default();
    test_server.start();
    sleep(std::time::Duration::from_secs(1)).await;
    let system_client = client_factory.create_client().await;
    let client1 = client_factory.create_client().await;
    let client2 = client_factory.create_client().await;
    let client3 = client_factory.create_client().await;
    let system_client = system_client.as_ref();
    let client1 = client1.as_ref();
    let client2 = client2.as_ref();
    let client3 = client3.as_ref();

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
    let create_group = CreateGroup {
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        group_id: GROUP_ID,
    };
    system_client.create_group(&create_group).await.unwrap();

    let join_group = JoinGroup {
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        group_id: GROUP_ID,
    };

    // 4. Join the consumer group by each client
    client1.join_group(&join_group).await.unwrap();
    client2.join_group(&join_group).await.unwrap();
    client3.join_group(&join_group).await.unwrap();

    // 5. Send messages to the calculated partition ID by using entity ID as a key
    for entity_id in 1..=MESSAGES_COUNT_PER_PARTITION * PARTITIONS_COUNT {
        let messages = vec![Message::from_str(&get_message_payload(entity_id)).unwrap()];
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
    let group_info = system_client
        .get_group(&GetGroup {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            group_id: GROUP_ID,
        })
        .await
        .unwrap();

    for member in &group_info.members {
        assert_eq!(member.partitions.len(), 1);
    }

    // 7. Poll the messages for each client per assigned partition in the consumer group
    validate_message_polling(client1, &group_info).await;
    validate_message_polling(client2, &group_info).await;
    validate_message_polling(client3, &group_info).await;

    test_server.stop();
}

async fn validate_message_polling(client: &dyn Client, group: &ConsumerGroupDetails) {
    let client_info = client.get_me(&GetMe {}).await.unwrap();
    let group_member = group
        .members
        .iter()
        .find(|m| m.id == client_info.id)
        .unwrap();
    let partition_id = group_member.partitions[0];
    let mut start_message_id = partition_id % PARTITIONS_COUNT;
    if start_message_id == 0 {
        start_message_id = PARTITIONS_COUNT;
    }

    let poll_messages = PollMessages {
        consumer_type: ConsumerType::Group,
        consumer_id: GROUP_ID,
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
        let message_id = start_message_id + ((i - 1) * PARTITIONS_COUNT);
        let payload = from_utf8(&message.payload).unwrap();
        assert_eq!(payload, &get_message_payload(message_id));
    }
}

fn get_message_payload(id: u32) -> String {
    format!("message-{}", id)
}
