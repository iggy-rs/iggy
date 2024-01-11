use crate::streaming::common::test_setup::TestSetup;
use iggy::identifier::Identifier;
use server::streaming::topics::topic::Topic;

#[tokio::test]
async fn should_persist_consumer_group_and_then_load_it_from_disk() {
    let setup = TestSetup::init().await;
    let storage = setup.storage.topic.as_ref();
    let mut topic = init_topic(&setup).await;
    let consumer_group_id = 1;
    let consumer_group_name = "test";
    topic
        .create_consumer_group(consumer_group_id, consumer_group_name)
        .await
        .unwrap();

    let consumer_groups = storage.load_consumer_groups(&topic).await.unwrap();
    assert_eq!(consumer_groups.len(), 1);
    let consumer_group = consumer_groups.first().unwrap();

    let consumer_group_by_id = topic
        .get_consumer_group(&Identifier::numeric(consumer_group_id).unwrap())
        .unwrap();
    let consumer_group_by_id = consumer_group_by_id.read().await;
    assert_eq!(
        consumer_group_by_id.consumer_group_id,
        consumer_group.consumer_group_id
    );
    assert_eq!(consumer_group_by_id.name, consumer_group.name);

    let consumer_group_by_name = topic
        .get_consumer_group(&Identifier::named(consumer_group_name).unwrap())
        .unwrap();
    let consumer_group_by_name = consumer_group_by_name.read().await;
    assert_eq!(
        consumer_group_by_name.consumer_group_id,
        consumer_group.consumer_group_id
    );
    assert_eq!(consumer_group_by_name.name, consumer_group.name);
}

#[tokio::test]
async fn should_delete_consumer_group_from_disk() {
    let setup = TestSetup::init().await;
    let storage = setup.storage.topic.as_ref();
    let mut topic = init_topic(&setup).await;
    let consumer_group_id = 1;
    let consumer_group_name = "test";
    topic
        .create_consumer_group(consumer_group_id, consumer_group_name)
        .await
        .unwrap();

    let consumer_groups = storage.load_consumer_groups(&topic).await.unwrap();
    assert_eq!(consumer_groups.len(), 1);
    let consumer_group = consumer_groups.first().unwrap();

    let deleted_consumer_group = topic
        .delete_consumer_group(&Identifier::numeric(consumer_group_id).unwrap())
        .await
        .unwrap();
    let deleted_consumer_group = deleted_consumer_group.read().await;
    assert_eq!(
        deleted_consumer_group.consumer_group_id,
        consumer_group.consumer_group_id
    );
    assert_eq!(deleted_consumer_group.name, consumer_group.name);
    let consumer_groups = storage.load_consumer_groups(&topic).await.unwrap();
    assert!(consumer_groups.is_empty());
}

async fn init_topic(setup: &TestSetup) -> Topic {
    let stream_id = 1;
    setup.create_topics_directory(stream_id).await;
    let name = "test";
    let topic = Topic::create(
        stream_id,
        1,
        name,
        1,
        setup.config.clone(),
        setup.storage.clone(),
        None,
        None,
        1,
    )
    .unwrap();
    topic.persist().await.unwrap();
    topic
}
