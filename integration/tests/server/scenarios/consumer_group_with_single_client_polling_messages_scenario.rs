use iggy::client::{ConsumerGroupClient, MessageClient, StreamClient, SystemClient, TopicClient};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::consumer::Consumer;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingStrategy};
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::system::get_me::GetMe;
use iggy::topics::create_topic::CreateTopic;
use integration::test_server::{assert_clean_system, login_root, ClientFactory};
use std::str::{from_utf8, FromStr};

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const CONSUMER_GROUP_ID: u32 = 10;
const CONSUMER_GROUP_NAME: &str = "test-consumer-group";
const MESSAGES_COUNT: u32 = 1000;

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, IggyClientConfig::default(), None, None, None);

    login_root(&client).await;
    init_system(&client).await;
    execute_using_messages_key_key(&client).await;
    client
        .delete_stream(&DeleteStream {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        })
        .await
        .unwrap();
    init_system(&client).await;
    execute_using_none_key(&client).await;
    cleanup(&client).await;
    assert_clean_system(&client).await;
}

async fn init_system(client: &IggyClient) {
    // 1. Create the stream
    let create_stream = CreateStream {
        stream_id: STREAM_ID,
        name: STREAM_NAME.to_string(),
    };
    client.create_stream(&create_stream).await.unwrap();

    // 2. Create the topic
    let create_topic = CreateTopic {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: TOPIC_ID,
        partitions_count: PARTITIONS_COUNT,
        name: TOPIC_NAME.to_string(),
        message_expiry: None,
    };
    client.create_topic(&create_topic).await.unwrap();

    // 3. Create the consumer group
    let create_group = CreateConsumerGroup {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        consumer_group_id: CONSUMER_GROUP_ID,
        name: CONSUMER_GROUP_NAME.to_string(),
    };
    client.create_consumer_group(&create_group).await.unwrap();

    let join_group = JoinConsumerGroup {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        consumer_group_id: Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
    };

    // 4. Join the consumer group by client
    client.join_consumer_group(&join_group).await.unwrap();

    // 5. Validate that group contains the single client with all partitions assigned
    let consumer_group_info = client
        .get_consumer_group(&GetConsumerGroup {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            consumer_group_id: Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
        })
        .await
        .unwrap();

    let client_info = client.get_me(&GetMe {}).await.unwrap();
    assert_eq!(consumer_group_info.id, CONSUMER_GROUP_ID);

    assert_eq!(consumer_group_info.members_count, 1);
    assert_eq!(consumer_group_info.members.len(), 1);
    let member = &consumer_group_info.members[0];
    assert_eq!(member.id, client_info.client_id);
    assert_eq!(member.partitions.len() as u32, PARTITIONS_COUNT);
    assert_eq!(member.partitions_count, PARTITIONS_COUNT);
}

async fn execute_using_messages_key_key(client: &IggyClient) {
    // 1. Send messages to the calculated partition ID on the server side by using entity ID as a key
    for entity_id in 1..=MESSAGES_COUNT {
        let message = Message::from_str(&get_message_payload(entity_id)).unwrap();
        let messages = vec![message];
        let mut send_messages = SendMessages {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            partitioning: Partitioning::messages_key_u32(entity_id),
            messages,
        };
        client.send_messages(&mut send_messages).await.unwrap();
    }

    // 2. Poll the messages for the single client which has assigned all partitions in the consumer group
    let poll_messages = PollMessages {
        consumer: Consumer::group(Identifier::numeric(CONSUMER_GROUP_ID).unwrap()),
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        partition_id: None,
        strategy: PollingStrategy::next(),
        count: 1,
        auto_commit: true,
    };

    let mut total_read_messages_count = 0;
    for _ in 1..=PARTITIONS_COUNT * MESSAGES_COUNT {
        let polled_messages = client.poll_messages(&poll_messages).await.unwrap();
        total_read_messages_count += polled_messages.messages.len() as u32;
    }

    assert_eq!(total_read_messages_count, MESSAGES_COUNT);
}

fn get_message_payload(entity_id: u32) -> String {
    format!("message-{}", entity_id)
}

async fn execute_using_none_key(client: &IggyClient) {
    // 1. Send messages to the calculated partition ID on the server side (round-robin) by using none key
    for entity_id in 1..=MESSAGES_COUNT * PARTITIONS_COUNT {
        let mut partition_id = entity_id % PARTITIONS_COUNT;
        if partition_id == 0 {
            partition_id = PARTITIONS_COUNT;
        }

        let message =
            Message::from_str(&get_extended_message_payload(partition_id, entity_id)).unwrap();
        let messages = vec![message];
        let mut send_messages = SendMessages {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            partitioning: Partitioning::balanced(),
            messages,
        };
        client.send_messages(&mut send_messages).await.unwrap();
    }

    // 2. Poll the messages for the single client which has assigned all partitions in the consumer group
    let poll_messages = PollMessages {
        consumer: Consumer::group(Identifier::numeric(CONSUMER_GROUP_ID).unwrap()),
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        partition_id: None,
        strategy: PollingStrategy::next(),
        count: 1,
        auto_commit: true,
    };

    let mut partition_id = 1;
    let mut offset = 0;
    let mut entity_id = 1;
    for _ in 1..=PARTITIONS_COUNT * MESSAGES_COUNT {
        let polled_messages = client.poll_messages(&poll_messages).await.unwrap();
        assert_eq!(polled_messages.messages.len(), 1);
        let message = &polled_messages.messages[0];
        assert_eq!(message.offset, offset);
        let payload = from_utf8(&message.payload).unwrap();
        assert_eq!(
            payload,
            &get_extended_message_payload(partition_id, entity_id)
        );
        partition_id += 1;
        entity_id += 1;
        if partition_id > PARTITIONS_COUNT {
            partition_id = 1;
            offset += 1;
        }
    }

    for _ in 1..=PARTITIONS_COUNT {
        let polled_messages = client.poll_messages(&poll_messages).await.unwrap();
        assert!(polled_messages.messages.is_empty());
    }
}

fn get_extended_message_payload(partition_id: u32, entity_id: u32) -> String {
    format!("message-{}-{}", partition_id, entity_id)
}

async fn cleanup(system_client: &IggyClient) {
    let delete_stream = DeleteStream {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
    };
    system_client.delete_stream(&delete_stream).await.unwrap();
}
