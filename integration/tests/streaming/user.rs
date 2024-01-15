use crate::streaming::common::test_setup::TestSetup;
use iggy::models::permissions::{
    GlobalPermissions, Permissions, StreamPermissions, TopicPermissions,
};
use iggy::models::user_status::UserStatus;
use iggy::utils::timestamp::IggyTimestamp;
use server::streaming::users::user::User;
use std::collections::HashMap;

#[tokio::test]
async fn single_user_should_be_saved_and_loaded() {
    let setup = TestSetup::init().await;
    let user_id = 10;
    let user = create_user(user_id);
    setup.storage.user.save(&user).await.unwrap();

    let mut loaded_user = User {
        id: user_id,
        ..Default::default()
    };
    setup.storage.user.load(&mut loaded_user).await.unwrap();

    assert_user(&user, &loaded_user);
}

#[tokio::test]
async fn many_users_should_be_saved_and_loaded() {
    let setup = TestSetup::init().await;
    let user1 = create_user(1);
    let user2 = create_user(2);
    let user3 = create_user(3);

    setup.storage.user.save(&user1).await.unwrap();
    setup.storage.user.save(&user2).await.unwrap();
    setup.storage.user.save(&user3).await.unwrap();

    let users = setup.storage.user.load_all().await.unwrap();
    assert_eq!(users.len(), 3);
    let loaded_user1 = users.first().unwrap();
    let loaded_user2 = users.get(1).unwrap();
    let loaded_user3 = users.get(2).unwrap();
    assert_user(&user1, loaded_user1);
    assert_user(&user2, loaded_user2);
    assert_user(&user3, loaded_user3);
}

#[tokio::test]
async fn user_should_be_deleted() {
    let setup = TestSetup::init().await;
    let user1 = create_user(1);
    let user2 = create_user(2);
    setup.storage.user.save(&user1).await.unwrap();
    setup.storage.user.save(&user2).await.unwrap();

    let users = setup.storage.user.load_all().await.unwrap();
    assert_eq!(users.len(), 2);

    setup.storage.user.delete(&user1).await.unwrap();
    let users = setup.storage.user.load_all().await.unwrap();
    assert_eq!(users.len(), 1);
    let loaded_user = users.first().unwrap();
    assert_user(&user2, loaded_user);
}

fn assert_user(user: &User, loaded_user: &User) {
    assert_eq!(loaded_user.id, user.id);
    assert_eq!(loaded_user.username, user.username);
    assert_eq!(loaded_user.password, user.password);
    assert_eq!(loaded_user.created_at, user.created_at);
    assert_eq!(loaded_user.status, user.status);
    if user.permissions.is_none() {
        assert!(loaded_user.permissions.is_none());
        return;
    }

    let user_permissions = user.permissions.as_ref().unwrap();
    let loaded_user_permissions = loaded_user.permissions.as_ref().unwrap();

    assert_eq!(
        loaded_user_permissions.global.manage_servers,
        user_permissions.global.manage_servers
    );
    assert_eq!(
        loaded_user_permissions.global.read_servers,
        user_permissions.global.read_servers
    );
    assert_eq!(
        loaded_user_permissions.global.manage_users,
        user_permissions.global.manage_users
    );
    assert_eq!(
        loaded_user_permissions.global.read_users,
        user_permissions.global.read_users
    );
    assert_eq!(
        loaded_user_permissions.global.manage_streams,
        user_permissions.global.manage_streams
    );
    assert_eq!(
        loaded_user_permissions.global.read_streams,
        user_permissions.global.read_streams
    );
    assert_eq!(
        loaded_user_permissions.global.manage_topics,
        user_permissions.global.manage_topics
    );
    assert_eq!(
        loaded_user_permissions.global.read_topics,
        user_permissions.global.read_topics
    );
    assert_eq!(
        loaded_user_permissions.global.poll_messages,
        user_permissions.global.poll_messages
    );
    assert_eq!(
        loaded_user_permissions.global.send_messages,
        user_permissions.global.send_messages
    );

    if user_permissions.streams.is_none() {
        assert!(loaded_user_permissions.streams.is_none());
        return;
    }

    let streams = user_permissions.streams.as_ref().unwrap();
    let loaded_streams = loaded_user_permissions.streams.as_ref().unwrap();

    assert_eq!(loaded_streams.len(), streams.len());
    for (stream_id, stream) in streams {
        let loaded_stream = loaded_streams.get(stream_id).unwrap();
        assert_eq!(loaded_stream.manage_stream, stream.manage_stream);
        assert_eq!(loaded_stream.read_stream, stream.read_stream);
        assert_eq!(loaded_stream.manage_topics, stream.manage_topics);
        assert_eq!(loaded_stream.read_topics, stream.read_topics);
        assert_eq!(loaded_stream.poll_messages, stream.poll_messages);
        assert_eq!(loaded_stream.send_messages, stream.send_messages);

        if stream.topics.is_none() {
            assert!(loaded_stream.topics.is_none());
            continue;
        }

        let topics = stream.topics.as_ref().unwrap();
        let loaded_topics = loaded_stream.topics.as_ref().unwrap();
        assert_eq!(loaded_topics.len(), topics.len());
        for (topic_id, topic) in topics {
            let loaded_topic = loaded_topics.get(topic_id).unwrap();
            assert_eq!(loaded_topic.manage_topic, topic.manage_topic);
            assert_eq!(loaded_topic.read_topic, topic.read_topic);
            assert_eq!(loaded_topic.poll_messages, topic.poll_messages);
            assert_eq!(loaded_topic.send_messages, topic.send_messages);
        }
    }
}

fn create_user(id: u32) -> User {
    User {
        id,
        username: format!("user{}", id),
        password: "secret".to_string(),
        created_at: IggyTimestamp::now().to_micros(),
        status: UserStatus::Active,
        permissions: Some(Permissions {
            global: GlobalPermissions {
                manage_servers: false,
                read_servers: false,
                manage_users: false,
                read_users: false,
                manage_streams: false,
                manage_topics: false,
                read_streams: true,
                poll_messages: false,
                send_messages: false,
                read_topics: true,
            },
            streams: Some({
                let mut map = HashMap::new();
                map.insert(
                    1,
                    StreamPermissions {
                        manage_stream: false,
                        read_stream: false,
                        manage_topics: false,
                        read_topics: true,
                        poll_messages: true,
                        send_messages: true,
                        topics: Some({
                            let mut map = HashMap::new();
                            map.insert(
                                1,
                                TopicPermissions {
                                    manage_topic: false,
                                    read_topic: true,
                                    poll_messages: true,
                                    send_messages: true,
                                },
                            );
                            map
                        }),
                    },
                );
                map
            }),
        }),
    }
}
