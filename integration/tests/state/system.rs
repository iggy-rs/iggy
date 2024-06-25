use crate::state::StateSetup;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::{
    CREATE_PARTITIONS_CODE, CREATE_PERSONAL_ACCESS_TOKEN_CODE, CREATE_STREAM_CODE,
    CREATE_TOPIC_CODE, CREATE_USER_CODE, DELETE_STREAM_CODE,
};
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::users::create_user::CreateUser;
use iggy::utils::expiry::IggyExpiry;
use server::state::models::CreatePersonalAccessTokenWithHash;
use server::state::system::SystemState;
use server::state::State;

#[tokio::test]
async fn should_be_initialized_based_on_state_entries() {
    let setup = StateSetup::init().await;
    let state = setup.state();
    state.init().await.unwrap();

    let user_id = 1;
    let create_user = CreateUser {
        username: "user".to_string(),
        password: "secret".to_string(),
        status: Default::default(),
        permissions: None,
    };

    let create_stream1 = CreateStream {
        stream_id: Some(1),
        name: "stream1".to_string(),
    };

    let create_topic1 = CreateTopic {
        stream_id: create_stream1.stream_id.unwrap().try_into().unwrap(),
        topic_id: Some(1),
        partitions_count: 1,
        compression_algorithm: Default::default(),
        message_expiry: Default::default(),
        max_topic_size: Default::default(),
        name: "topic1".to_string(),
        replication_factor: None,
    };

    let create_stream2 = CreateStream {
        stream_id: Some(2),
        name: "stream2".to_string(),
    };

    let create_topic2 = CreateTopic {
        stream_id: create_stream2.stream_id.unwrap().try_into().unwrap(),
        topic_id: Some(2),
        partitions_count: 1,
        compression_algorithm: Default::default(),
        message_expiry: Default::default(),
        max_topic_size: Default::default(),
        name: "topic2".to_string(),
        replication_factor: None,
    };

    let create_partitions = CreatePartitions {
        stream_id: create_topic1.stream_id.clone(),
        topic_id: create_topic1.topic_id.unwrap().try_into().unwrap(),
        partitions_count: 2,
    };

    let delete_stream2 = DeleteStream {
        stream_id: create_stream2.stream_id.unwrap().try_into().unwrap(),
    };

    let create_personal_access_token = CreatePersonalAccessTokenWithHash {
        command: CreatePersonalAccessToken {
            name: "test".to_string(),
            expiry: IggyExpiry::NeverExpire,
        },
        hash: "hash".to_string(),
    };

    state
        .apply(CREATE_USER_CODE, user_id, &create_user.as_bytes(), None)
        .await
        .unwrap();
    state
        .apply(
            CREATE_STREAM_CODE,
            user_id,
            &create_stream1.as_bytes(),
            None,
        )
        .await
        .unwrap();
    state
        .apply(CREATE_TOPIC_CODE, user_id, &create_topic1.as_bytes(), None)
        .await
        .unwrap();
    state
        .apply(
            CREATE_STREAM_CODE,
            user_id,
            &create_stream2.as_bytes(),
            None,
        )
        .await
        .unwrap();
    state
        .apply(CREATE_TOPIC_CODE, user_id, &create_topic2.as_bytes(), None)
        .await
        .unwrap();
    state
        .apply(
            CREATE_PARTITIONS_CODE,
            user_id,
            &create_partitions.as_bytes(),
            None,
        )
        .await
        .unwrap();
    state
        .apply(
            DELETE_STREAM_CODE,
            user_id,
            &delete_stream2.as_bytes(),
            None,
        )
        .await
        .unwrap();
    state
        .apply(
            CREATE_PERSONAL_ACCESS_TOKEN_CODE,
            user_id,
            &create_personal_access_token.as_bytes(),
            None,
        )
        .await
        .unwrap();

    let entries = state.load_entries().await.unwrap();
    let mut system = SystemState::init(entries).await.unwrap();

    assert_eq!(system.users.len(), 1);
    let mut user = system.users.remove(&1).unwrap();
    assert_eq!(user.id, 1);
    assert_eq!(user.username, create_user.username);
    assert_eq!(user.password_hash, create_user.password);
    assert_eq!(user.personal_access_tokens.len(), 1);

    let personal_access_token = user
        .personal_access_tokens
        .remove(&create_personal_access_token.command.name)
        .unwrap();
    assert_eq!(
        personal_access_token.token_hash,
        create_personal_access_token.hash
    );

    assert_eq!(system.streams.len(), 1);
    let mut stream = system
        .streams
        .remove(&create_stream1.stream_id.unwrap())
        .unwrap();
    assert_eq!(stream.name, create_stream1.name);
    assert_eq!(stream.topics.len(), 1);

    let topic = stream
        .topics
        .remove(&create_topic1.topic_id.unwrap())
        .unwrap();
    assert_eq!(topic.name, create_topic1.name);
    assert_eq!(topic.partitions.len(), 3);
}
