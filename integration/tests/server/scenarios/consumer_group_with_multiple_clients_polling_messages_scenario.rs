use iggy::client::{ConsumerGroupClient, MessageClient, StreamClient, SystemClient, TopicClient};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::consumer::Consumer;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingStrategy};
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy::models::consumer_group::ConsumerGroupDetails;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::system::get_me::GetMe;
use iggy::topics::create_topic::CreateTopic;
use integration::test_server::{
    assert_clean_system, create_user, delete_user, login_root, login_user, ClientFactory,
};
use std::str::{from_utf8, FromStr};

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const CONSUMER_GROUP_ID: u32 = 10;
const CONSUMER_GROUP_NAME: &str = "test-consumer-group";
const MESSAGES_COUNT: u32 = 1000;
const USERNAME_1: &str = "user1";
const USERNAME_2: &str = "user2";
const USERNAME_3: &str = "user3";

pub async fn run(client_factory: &dyn ClientFactory) {
    let system_client = create_client(client_factory).await;
    let client1 = create_client(client_factory).await;
    let client2 = create_client(client_factory).await;
    let client3 = create_client(client_factory).await;
    login_root(&system_client).await;
    init_system(&system_client, &client1, &client2, &client3, true).await;
    execute_using_messages_key_key(&system_client, &client1, &client2, &client3).await;
    cleanup(&system_client, false).await;
    init_system(&system_client, &client1, &client2, &client3, false).await;
    execute_using_none_key(&system_client, &client1, &client2, &client3).await;
    cleanup(&system_client, true).await;
    assert_clean_system(&system_client).await;
}

async fn create_client(client_factory: &dyn ClientFactory) -> IggyClient {
    let client = client_factory.create_client().await;
    IggyClient::create(client, IggyClientConfig::default(), None, None, None)
}

async fn init_system(
    system_client: &IggyClient,
    client1: &IggyClient,
    client2: &IggyClient,
    client3: &IggyClient,
    create_users: bool,
) {
    // 1. Create the stream
    let create_stream = CreateStream {
        stream_id: STREAM_ID,
        name: STREAM_NAME.to_string(),
    };
    system_client.create_stream(&create_stream).await.unwrap();

    // 2. Create the topic
    let create_topic = CreateTopic {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: TOPIC_ID,
        partitions_count: PARTITIONS_COUNT,
        name: TOPIC_NAME.to_string(),
        message_expiry: None,
    };
    system_client.create_topic(&create_topic).await.unwrap();

    // 3. Create the consumer group
    let create_group = CreateConsumerGroup {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        consumer_group_id: CONSUMER_GROUP_ID,
        name: CONSUMER_GROUP_NAME.to_string(),
    };
    system_client
        .create_consumer_group(&create_group)
        .await
        .unwrap();

    if create_users {
        // Create the users for all clients
        create_user(system_client, "user1").await;
        create_user(system_client, "user2").await;
        create_user(system_client, "user3").await;

        // Login all the clients
        login_user(client1, "user1").await;
        login_user(client2, "user2").await;
        login_user(client3, "user3").await;
    }

    let join_group = JoinConsumerGroup {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        consumer_group_id: Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
    };

    // 4. Join the consumer group by each client
    client1.join_consumer_group(&join_group).await.unwrap();
    client2.join_consumer_group(&join_group).await.unwrap();
    client3.join_consumer_group(&join_group).await.unwrap();

    // 5. Get the consumer group details
    let consumer_group_info = system_client
        .get_consumer_group(&GetConsumerGroup {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            consumer_group_id: Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
        })
        .await
        .unwrap();

    for member in &consumer_group_info.members {
        assert_eq!(member.partitions.len(), 1);
    }
}

async fn execute_using_messages_key_key(
    system_client: &IggyClient,
    client1: &IggyClient,
    client2: &IggyClient,
    client3: &IggyClient,
) {
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
        system_client
            .send_messages(&mut send_messages)
            .await
            .unwrap();
    }

    // 2. Poll the messages for each client per assigned partition in the consumer group
    let mut total_read_messages_count = 0;
    total_read_messages_count += poll_messages(client1).await;
    total_read_messages_count += poll_messages(client2).await;
    total_read_messages_count += poll_messages(client3).await;

    assert_eq!(total_read_messages_count, MESSAGES_COUNT);
}

async fn poll_messages(client: &IggyClient) -> u32 {
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

    total_read_messages_count
}

fn get_message_payload(entity_id: u32) -> String {
    format!("message-{}", entity_id)
}

async fn execute_using_none_key(
    system_client: &IggyClient,
    client1: &IggyClient,
    client2: &IggyClient,
    client3: &IggyClient,
) {
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
        system_client
            .send_messages(&mut send_messages)
            .await
            .unwrap();
    }

    let consumer_group_info = system_client
        .get_consumer_group(&GetConsumerGroup {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            consumer_group_id: Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
        })
        .await
        .unwrap();

    for member in &consumer_group_info.members {
        assert_eq!(member.partitions.len(), 1);
    }

    // 2. Poll the messages for each client per assigned partition in the consumer group
    validate_message_polling(client1, &consumer_group_info).await;
    validate_message_polling(client2, &consumer_group_info).await;
    validate_message_polling(client3, &consumer_group_info).await;
}

async fn validate_message_polling(client: &IggyClient, consumer_group: &ConsumerGroupDetails) {
    let client_info = client.get_me(&GetMe {}).await.unwrap();
    let consumer_group_member = consumer_group
        .members
        .iter()
        .find(|m| m.id == client_info.client_id)
        .unwrap();
    let partition_id = consumer_group_member.partitions[0];
    let mut start_entity_id = partition_id % PARTITIONS_COUNT;
    if start_entity_id == 0 {
        start_entity_id = PARTITIONS_COUNT;
    }

    let poll_messages = PollMessages {
        consumer: Consumer::group(Identifier::numeric(CONSUMER_GROUP_ID).unwrap()),
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        partition_id: None,
        strategy: PollingStrategy::next(),
        count: 1,
        auto_commit: true,
    };

    for i in 1..=MESSAGES_COUNT {
        let polled_messages = client.poll_messages(&poll_messages).await.unwrap();
        assert_eq!(polled_messages.messages.len(), 1);
        let message = &polled_messages.messages[0];
        let offset = (i - 1) as u64;
        assert_eq!(message.offset, offset);
        let entity_id = start_entity_id + ((i - 1) * PARTITIONS_COUNT);
        let payload = from_utf8(&message.payload).unwrap();
        assert_eq!(
            payload,
            &get_extended_message_payload(partition_id, entity_id)
        );
    }

    let polled_messages = client.poll_messages(&poll_messages).await.unwrap();
    assert!(polled_messages.messages.is_empty())
}

fn get_extended_message_payload(partition_id: u32, entity_id: u32) -> String {
    format!("message-{}-{}", partition_id, entity_id)
}

async fn cleanup(system_client: &IggyClient, delete_users: bool) {
    if delete_users {
        delete_user(system_client, USERNAME_1).await;
        delete_user(system_client, USERNAME_2).await;
        delete_user(system_client, USERNAME_3).await;
    }
    let delete_stream = DeleteStream {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
    };
    system_client.delete_stream(&delete_stream).await.unwrap();
}
