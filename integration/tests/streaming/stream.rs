use crate::streaming::common::test_setup::TestSetup;
use crate::streaming::create_messages;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::messages::send_messages::Partitioning;
use server::streaming::polling_consumer::PollingConsumer;
use server::streaming::streams::stream::Stream;
use tokio::fs;

#[tokio::test]
async fn should_persist_stream_with_topics_directory_and_info_file() {
    let setup = TestSetup::init().await;
    setup.create_streams_directory().await;
    let stream_ids = get_stream_ids();
    for stream_id in stream_ids {
        let name = format!("test-{}", stream_id);
        let stream = Stream::create(
            stream_id,
            &name,
            setup.config.clone(),
            setup.storage.clone(),
        );

        stream.persist().await.unwrap();

        assert_persisted_stream(&stream.path, &setup.config.topic.path).await;
    }
}

#[tokio::test]
async fn should_load_existing_stream_from_disk() {
    let setup = TestSetup::init().await;
    setup.create_streams_directory().await;
    let stream_ids = get_stream_ids();
    for stream_id in stream_ids {
        let name = format!("test-{}", stream_id);
        let stream = Stream::create(
            stream_id,
            &name,
            setup.config.clone(),
            setup.storage.clone(),
        );
        stream.persist().await.unwrap();
        assert_persisted_stream(&stream.path, &setup.config.topic.path).await;

        let mut loaded_stream =
            Stream::empty(stream_id, setup.config.clone(), setup.storage.clone());
        loaded_stream.load().await.unwrap();

        assert_eq!(loaded_stream.stream_id, stream.stream_id);
        assert_eq!(loaded_stream.name, stream.name);
        assert_eq!(loaded_stream.path, stream.path);
        assert_eq!(loaded_stream.topics_path, stream.topics_path);
    }
}

#[tokio::test]
async fn should_delete_existing_stream_from_disk() {
    let setup = TestSetup::init().await;
    setup.create_streams_directory().await;
    let stream_ids = get_stream_ids();
    for stream_id in stream_ids {
        let name = format!("test-{}", stream_id);
        let stream = Stream::create(
            stream_id,
            &name,
            setup.config.clone(),
            setup.storage.clone(),
        );
        stream.persist().await.unwrap();
        assert_persisted_stream(&stream.path, &setup.config.topic.path).await;

        stream.delete().await.unwrap();

        assert!(fs::metadata(&stream.path).await.is_err());
    }
}

#[tokio::test]
async fn should_purge_existing_stream_on_disk() {
    let setup = TestSetup::init().await;
    setup.create_streams_directory().await;
    let stream_ids = get_stream_ids();
    for stream_id in stream_ids {
        let name = format!("test-{}", stream_id);
        let mut stream = Stream::create(
            stream_id,
            &name,
            setup.config.clone(),
            setup.storage.clone(),
        );
        stream.persist().await.unwrap();
        assert_persisted_stream(&stream.path, &setup.config.topic.path).await;

        let topic_id = 1;
        stream
            .create_topic(topic_id, "test", 1, None, None, 1)
            .await
            .unwrap();

        let messages = create_messages();
        let messages_count = messages.len();
        let topic = stream
            .get_topic(&Identifier::numeric(topic_id).unwrap())
            .unwrap();
        topic
            .append_messages(&Partitioning::partition_id(1), messages)
            .await
            .unwrap();
        let loaded_messages = topic
            .get_messages(
                PollingConsumer::Consumer(1, 1),
                1,
                PollingStrategy::offset(0),
                100,
            )
            .await
            .unwrap();

        assert_eq!(loaded_messages.messages.len(), messages_count);

        stream.purge().await.unwrap();
        let loaded_messages = topic
            .get_messages(
                PollingConsumer::Consumer(1, 1),
                1,
                PollingStrategy::offset(0),
                100,
            )
            .await
            .unwrap();
        assert_eq!(loaded_messages.current_offset, 0);
        assert!(loaded_messages.messages.is_empty());
    }
}

async fn assert_persisted_stream(stream_path: &str, topics_directory: &str) {
    let stream_metadata = fs::metadata(stream_path).await.unwrap();
    assert!(stream_metadata.is_dir());
    let topics_path = format!("{}/{}", stream_path, topics_directory);
    let topics_metadata = fs::metadata(&topics_path).await.unwrap();
    assert!(topics_metadata.is_dir());
}

fn get_stream_ids() -> Vec<u32> {
    vec![1, 2, 3, 5, 10, 100, 1000, 99999]
}
