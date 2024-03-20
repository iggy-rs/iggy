use iggy::client_v2::{ConsumerGroupClientV2, StreamClientV2};
use iggy::clients::client_v2::{IggyClientBackgroundConfigV2, IggyClientV2};
use iggy::consumer::ConsumerKind;
use iggy::identifier::Identifier;
use iggy::models::consumer_group::ConsumerGroupDetails;
use integration::test_server::{delete_user_v2, ClientFactoryV2};

pub mod consumer_group_join_scenario;
pub mod consumer_group_with_multiple_clients_polling_messages_scenario;
pub mod consumer_group_with_single_client_polling_messages_scenario;
pub mod message_headers_scenario;
pub mod stream_size_validation_scenario;
pub mod system_scenario;
pub mod user_scenario;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const CONSUMER_GROUP_ID: u32 = 10;
const CONSUMER_GROUP_NAME: &str = "test-consumer-group";
const USERNAME_1: &str = "user1";
const USERNAME_2: &str = "user2";
const USERNAME_3: &str = "user3";
const CONSUMER_ID: u32 = 1;
const CONSUMER_KIND: ConsumerKind = ConsumerKind::Consumer;
const MESSAGES_COUNT: u32 = 1000;

async fn create_client(client_factory: &dyn ClientFactoryV2) -> IggyClientV2 {
    let client = client_factory.create_client().await;
    IggyClientV2::create(
        client,
        IggyClientBackgroundConfigV2::default(),
        None,
        None,
        None,
    )
}

async fn get_consumer_group(client: &IggyClientV2) -> ConsumerGroupDetails {
    client
        .get_consumer_group(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
            &Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
        )
        .await
        .unwrap()
}

async fn join_consumer_group(client: &IggyClientV2) {
    client
        .join_consumer_group(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
            &Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
        )
        .await
        .unwrap();
}

async fn leave_consumer_group(client: &IggyClientV2) {
    client
        .leave_consumer_group(
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
            &Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
        )
        .await
        .unwrap();
}

async fn cleanup(system_client: &IggyClientV2, delete_users: bool) {
    if delete_users {
        delete_user_v2(system_client, USERNAME_1).await;
        delete_user_v2(system_client, USERNAME_2).await;
        delete_user_v2(system_client, USERNAME_3).await;
    }
    system_client
        .delete_stream(&Identifier::numeric(STREAM_ID).unwrap())
        .await
        .unwrap();
}
