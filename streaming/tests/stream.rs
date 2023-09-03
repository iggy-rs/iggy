mod common;

use crate::common::TestSetup;
use streaming::streams::stream::Stream;
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

        assert_eq!(loaded_stream.id, stream.id);
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
