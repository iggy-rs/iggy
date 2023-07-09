use crate::common::{ClientFactory, TestServer};
use sdk::client::Client;
use sdk::groups::create_group::CreateGroup;
use sdk::groups::get_group::GetGroup;
use sdk::groups::join_group::JoinGroup;
use sdk::models::client_info::ClientInfoDetails;
use sdk::models::consumer_group::ConsumerGroupDetails;
use sdk::streams::create_stream::CreateStream;
use sdk::system::get_me::GetMe;
use sdk::topics::create_topic::CreateTopic;
use tokio::time::sleep;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const GROUP_ID: u32 = 1;

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

    // 4. Join the consumer group by client 1
    join_consumer_group(client1).await;

    // 5. Get client1 info and validate that it contains the single consumer group
    let client1_info = get_me_and_validate_consumer_groups(client1).await;

    // 6. Validate that the consumer group has 1 member and this member has all partitions assigned
    let group = get_consumer_group_and_validate_members(system_client, 1).await;
    let member = &group.members[0];
    assert_eq!(member.id, client1_info.id);
    assert_eq!(member.partitions_count, PARTITIONS_COUNT);
    assert_eq!(member.partitions.len() as u32, PARTITIONS_COUNT);

    // 7. Join the consumer group by client 2
    join_consumer_group(client2).await;

    // 8. Validate that client 2 contains the single consumer group
    get_me_and_validate_consumer_groups(client2).await;

    // 9. Validate that the consumer group has 2 members and partitions are distributed between them
    let group = get_consumer_group_and_validate_members(system_client, 2).await;
    let member1 = &group.members[0];
    let member2 = &group.members[1];
    assert!(member1.partitions_count >= 1 && member1.partitions_count < PARTITIONS_COUNT);
    assert!(member2.partitions_count >= 1 && member2.partitions_count < PARTITIONS_COUNT);
    assert_eq!(
        member1.partitions_count + member2.partitions_count,
        PARTITIONS_COUNT
    );

    // 10. Join the consumer group by client 3
    join_consumer_group(client3).await;

    // 11. Validate that client 3 contains the single consumer group
    get_me_and_validate_consumer_groups(client3).await;

    // 12. Validate that the consumer group has 3 members and partitions are equally distributed between them
    let group = get_consumer_group_and_validate_members(system_client, 3).await;
    let member1 = &group.members[0];
    let member2 = &group.members[1];
    let member3 = &group.members[2];
    assert_eq!(member1.partitions_count, 1);
    assert_eq!(member2.partitions_count, 1);
    assert_eq!(member3.partitions_count, 1);

    test_server.stop();
}

async fn join_consumer_group(client: &dyn Client) {
    let join_group = JoinGroup {
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        group_id: GROUP_ID,
    };
    client.join_group(&join_group).await.unwrap();
}

async fn get_me_and_validate_consumer_groups(client: &dyn Client) -> ClientInfoDetails {
    let get_me = GetMe {};
    let client_info = client.get_me(&get_me).await.unwrap();

    assert!(client_info.id > 0);
    assert_eq!(client_info.consumer_groups_count, 1);
    assert_eq!(client_info.consumer_groups.len(), 1);

    let consumer_group = &client_info.consumer_groups[0];
    assert_eq!(consumer_group.stream_id, STREAM_ID);
    assert_eq!(consumer_group.topic_id, TOPIC_ID);
    assert_eq!(consumer_group.group_id, GROUP_ID);

    client_info
}

async fn get_consumer_group_and_validate_members(
    client: &dyn Client,
    members_count: u32,
) -> ConsumerGroupDetails {
    let get_group = GetGroup {
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        group_id: GROUP_ID,
    };
    let group = client.get_group(&get_group).await.unwrap();

    assert_eq!(group.id, GROUP_ID);
    assert_eq!(group.partitions_count, PARTITIONS_COUNT);
    assert_eq!(group.members_count, members_count);
    assert_eq!(group.members.len() as u32, members_count);

    group
}
