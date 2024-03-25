use crate::server::scenarios::next::{
    cleanup, create_client, get_consumer_group, join_consumer_group, CONSUMER_GROUP_ID,
    CONSUMER_GROUP_NAME, MESSAGES_COUNT, PARTITIONS_COUNT, STREAM_ID, STREAM_NAME, TOPIC_ID,
    TOPIC_NAME,
};
use iggy::clients::next_client::IggyClientNext;
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::models::consumer_group::ConsumerGroupDetails;
use iggy::next_client::{
    ConsumerGroupClientNext, MessageClientNext, StreamClientNext, SystemClientNext, TopicClientNext,
};
use iggy::utils::expiry::IggyExpiry;
use integration::test_server::{
    assert_clean_system_next, create_user_next, login_root_next, login_user_next, ClientFactoryNext,
};
use std::str::{from_utf8, FromStr};

pub async fn run(client_factory: &dyn ClientFactoryNext) {
    let system_client = create_client(client_factory).await;
    let client1 = create_client(client_factory).await;
    let client2 = create_client(client_factory).await;
    let client3 = create_client(client_factory).await;
    login_root_next(&system_client).await;
    init_system(&system_client, &client1, &client2, &client3, true).await;
    execute_using_messages_key_key(&system_client, &client1, &client2, &client3).await;
    cleanup(&system_client, false).await;
    init_system(&system_client, &client1, &client2, &client3, false).await;
    execute_using_none_key(&system_client, &client1, &client2, &client3).await;
    cleanup(&system_client, true).await;
    assert_clean_system_next(&system_client).await;
}

async fn init_system(
    system_client: &IggyClientNext,
    client1: &IggyClientNext,
    client2: &IggyClientNext,
    client3: &IggyClientNext,
    create_users: bool,
) {
    // 1. Create the stream
    system_client
        .create_stream(STREAM_NAME, Some(STREAM_ID))
        .await
        .unwrap();

    // 2. Create the topic
    system_client
        .create_topic(
            &Identifier::numeric(STREAM_ID).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            Some(1),
            Some(TOPIC_ID),
            IggyExpiry::NeverExpire,
            None,
        )
        .await
        .unwrap();

    // 3. Create the consumer group
    system_client
        .create_consumer_group(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
            CONSUMER_GROUP_NAME,
            Some(CONSUMER_GROUP_ID),
        )
        .await
        .unwrap();

    if create_users {
        // Create the users for all clients
        create_user_next(system_client, "user1").await;
        create_user_next(system_client, "user2").await;
        create_user_next(system_client, "user3").await;

        // Login all the clients
        login_user_next(client1, "user1").await;
        login_user_next(client2, "user2").await;
        login_user_next(client3, "user3").await;
    }

    // 4. Join the consumer group by each client
    join_consumer_group(client1).await;
    join_consumer_group(client2).await;
    join_consumer_group(client3).await;

    // 5. Get the consumer group details
    let consumer_group_info = get_consumer_group(system_client).await;
    for member in &consumer_group_info.members {
        assert_eq!(member.partitions.len(), 1);
    }
}

async fn execute_using_messages_key_key(
    system_client: &IggyClientNext,
    client1: &IggyClientNext,
    client2: &IggyClientNext,
    client3: &IggyClientNext,
) {
    // 1. Send messages to the calculated partition ID on the server side by using entity ID as a key
    for entity_id in 1..=MESSAGES_COUNT {
        let message = Message::from_str(&create_message_payload(entity_id)).unwrap();
        let mut messages = vec![message];
        system_client
            .send_messages(
                &Identifier::numeric(STREAM_ID).unwrap(),
                &Identifier::numeric(TOPIC_ID).unwrap(),
                &Partitioning::messages_key_u32(entity_id),
                &mut messages,
            )
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

async fn poll_messages(client: &IggyClientNext) -> u32 {
    let consumer = Consumer::group(Identifier::numeric(CONSUMER_GROUP_ID).unwrap());
    let mut total_read_messages_count = 0;
    for _ in 1..=PARTITIONS_COUNT * MESSAGES_COUNT {
        let polled_messages = client
            .poll_messages(
                &Identifier::numeric(STREAM_ID).unwrap(),
                &Identifier::numeric(TOPIC_ID).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                true,
            )
            .await
            .unwrap();

        total_read_messages_count += polled_messages.messages.len() as u32;
    }

    total_read_messages_count
}

fn create_message_payload(entity_id: u32) -> String {
    format!("message-{}", entity_id)
}

async fn execute_using_none_key(
    system_client: &IggyClientNext,
    client1: &IggyClientNext,
    client2: &IggyClientNext,
    client3: &IggyClientNext,
) {
    // 1. Send messages to the calculated partition ID on the server side (round-robin) by using none key
    for entity_id in 1..=MESSAGES_COUNT * PARTITIONS_COUNT {
        let mut partition_id = entity_id % PARTITIONS_COUNT;
        if partition_id == 0 {
            partition_id = PARTITIONS_COUNT;
        }

        let message =
            Message::from_str(&create_extended_message_payload(partition_id, entity_id)).unwrap();
        let mut messages = vec![message];
        system_client
            .send_messages(
                &Identifier::numeric(STREAM_ID).unwrap(),
                &Identifier::numeric(TOPIC_ID).unwrap(),
                &Partitioning::balanced(),
                &mut messages,
            )
            .await
            .unwrap();
    }

    let consumer_group_info = get_consumer_group(system_client).await;
    for member in &consumer_group_info.members {
        assert_eq!(member.partitions.len(), 1);
    }

    // 2. Poll the messages for each client per assigned partition in the consumer group
    validate_message_polling(client1, &consumer_group_info).await;
    validate_message_polling(client2, &consumer_group_info).await;
    validate_message_polling(client3, &consumer_group_info).await;
}

async fn validate_message_polling(client: &IggyClientNext, consumer_group: &ConsumerGroupDetails) {
    let consumer = Consumer::group(Identifier::numeric(CONSUMER_GROUP_ID).unwrap());
    let client_info = client.get_me().await.unwrap();
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

    for i in 1..=MESSAGES_COUNT {
        let polled_messages = client
            .poll_messages(
                &Identifier::numeric(STREAM_ID).unwrap(),
                &Identifier::numeric(TOPIC_ID).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                true,
            )
            .await
            .unwrap();
        assert_eq!(polled_messages.messages.len(), 1);
        let message = &polled_messages.messages[0];
        let offset = (i - 1) as u64;
        assert_eq!(message.offset, offset);
        let entity_id = start_entity_id + ((i - 1) * PARTITIONS_COUNT);
        let payload = from_utf8(&message.payload).unwrap();
        assert_eq!(
            payload,
            &create_extended_message_payload(partition_id, entity_id)
        );
    }

    let polled_messages = client
        .poll_messages(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            true,
        )
        .await
        .unwrap();
    assert!(polled_messages.messages.is_empty())
}

fn create_extended_message_payload(partition_id: u32, entity_id: u32) -> String {
    format!("message-{}-{}", partition_id, entity_id)
}
