use crate::common::{ClientFactory, TestServer};
use iggy::client::{ConsumerGroupClient, StreamClient, SystemClient, TopicClient};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::identifier::Identifier;
use iggy::models::client_info::ClientInfoDetails;
use iggy::models::consumer_group::ConsumerGroupDetails;
use iggy::streams::create_stream::CreateStream;
use iggy::system::get_me::GetMe;
use iggy::topics::create_topic::CreateTopic;
use tokio::time::sleep;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const CONSUMER_GROUP_ID: u32 = 1;

#[allow(dead_code)]
pub async fn run(client_factory: &dyn ClientFactory) {
    let mut test_server = TestServer::default();
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
    };
    system_client
        .create_consumer_group(&create_group)
        .await
        .unwrap();

    // 4. Join the consumer group by client 1
    join_consumer_group(&client1).await;

    // 5. Get client1 info and validate that it contains the single consumer group
    let client1_info = get_me_and_validate_consumer_groups(&client1).await;

    // 6. Validate that the consumer group has 1 member and this member has all partitions assigned
    let consumer_group = get_consumer_group_and_validate_members(&system_client, 1).await;
    let member = &consumer_group.members[0];
    assert_eq!(member.id, client1_info.id);
    assert_eq!(member.partitions_count, PARTITIONS_COUNT);
    assert_eq!(member.partitions.len() as u32, PARTITIONS_COUNT);

    // 7. Join the consumer group by client 2
    join_consumer_group(&client2).await;

    // 8. Validate that client 2 contains the single consumer group
    get_me_and_validate_consumer_groups(&client2).await;

    // 9. Validate that the consumer group has 2 members and partitions are distributed between them
    let consumer_group = get_consumer_group_and_validate_members(&system_client, 2).await;
    let member1 = &consumer_group.members[0];
    let member2 = &consumer_group.members[1];
    assert!(member1.partitions_count >= 1 && member1.partitions_count < PARTITIONS_COUNT);
    assert!(member2.partitions_count >= 1 && member2.partitions_count < PARTITIONS_COUNT);
    assert_eq!(
        member1.partitions_count + member2.partitions_count,
        PARTITIONS_COUNT
    );

    // 10. Join the consumer group by client 3
    join_consumer_group(&client3).await;

    // 11. Validate that client 3 contains the single consumer group
    get_me_and_validate_consumer_groups(&client3).await;

    // 12. Validate that the consumer group has 3 members and partitions are equally distributed between them
    let consumer_group = get_consumer_group_and_validate_members(&system_client, 3).await;
    let member1 = &consumer_group.members[0];
    let member2 = &consumer_group.members[1];
    let member3 = &consumer_group.members[2];
    assert_eq!(member1.partitions_count, 1);
    assert_eq!(member2.partitions_count, 1);
    assert_eq!(member3.partitions_count, 1);
    assert_ne!(member1.partitions[0], member2.partitions[0]);
    assert_ne!(member1.partitions[0], member3.partitions[0]);
    assert_ne!(member2.partitions[0], member3.partitions[0]);

    test_server.stop();
}

async fn join_consumer_group(client: &IggyClient) {
    let join_group = JoinConsumerGroup {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        consumer_group_id: CONSUMER_GROUP_ID,
    };
    client.join_consumer_group(&join_group).await.unwrap();
}

async fn get_me_and_validate_consumer_groups(client: &IggyClient) -> ClientInfoDetails {
    let get_me = GetMe {};
    let client_info = client.get_me(&get_me).await.unwrap();

    assert!(client_info.id > 0);
    assert_eq!(client_info.consumer_groups_count, 1);
    assert_eq!(client_info.consumer_groups.len(), 1);

    let consumer_group = &client_info.consumer_groups[0];
    assert_eq!(consumer_group.stream_id, STREAM_ID);
    assert_eq!(consumer_group.topic_id, TOPIC_ID);
    assert_eq!(consumer_group.consumer_group_id, CONSUMER_GROUP_ID);

    client_info
}

async fn get_consumer_group_and_validate_members(
    client: &IggyClient,
    members_count: u32,
) -> ConsumerGroupDetails {
    let get_group = GetConsumerGroup {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        consumer_group_id: CONSUMER_GROUP_ID,
    };
    let consumer_group = client.get_consumer_group(&get_group).await.unwrap();

    assert_eq!(consumer_group.id, CONSUMER_GROUP_ID);
    assert_eq!(consumer_group.partitions_count, PARTITIONS_COUNT);
    assert_eq!(consumer_group.members_count, members_count);
    assert_eq!(consumer_group.members.len() as u32, members_count);

    consumer_group
}

async fn create_client(client_factory: &dyn ClientFactory) -> IggyClient {
    let client = client_factory.create_client().await;
    IggyClient::new(client, IggyClientConfig::default(), None, None)
}
