mod common;

use tokio::fs;
use streaming::streams::stream::{Stream, STREAM_INFO};
use crate::common::TestSetup;

#[tokio::test]
async fn should_persist_stream_with_topics_directory_and_info_file() {
    let setup = TestSetup::init().await;
    let id = 1;
    let name = "test";
    let stream = Stream::create(id, name, &setup.path, setup.config.stream.clone());
    
    stream.persist().await.unwrap();

    assert_persisted_stream(&setup.path, &stream.path, &setup.config.stream.topic.path, id).await;
}

#[tokio::test]
async fn should_load_existing_stream_from_disk() {
    let setup = TestSetup::init().await;
    let id = 1;
    let name = "test";
    let stream = Stream::create(id, name, &setup.path, setup.config.stream.clone());
    stream.persist().await.unwrap();
    assert_persisted_stream(&setup.path, &stream.path, &setup.config.stream.topic.path, id).await;
    
    let mut loaded_stream = Stream::empty(id, &setup.path, setup.config.stream.clone());
    loaded_stream.load().await.unwrap();
    
    assert_eq!(loaded_stream.id, stream.id);
    assert_eq!(loaded_stream.name, stream.name);
    assert_eq!(loaded_stream.path, stream.path);
    assert_eq!(loaded_stream.topics_path, stream.topics_path);
    assert_eq!(loaded_stream.info_path, stream.info_path);
}

#[tokio::test]
async fn should_delete_existing_stream_from_disk() {
    let setup = TestSetup::init().await;
    let id = 1;
    let name = "test";
    let stream = Stream::create(id, name, &setup.path, setup.config.stream.clone());
    stream.persist().await.unwrap();
    assert_persisted_stream(&setup.path, &stream.path, &setup.config.stream.topic.path, id).await;

    stream.delete().await.unwrap();
    let mut streams_dir_entries = fs::read_dir(&setup.path).await.unwrap();
    let entry = streams_dir_entries.next_entry().await.unwrap();
    assert!(entry.is_none());
}

async fn assert_persisted_stream(base_path: &str, stream_path: &str, topics_directory: &str, stream_id: u32) {
    let mut streams_dir_entries = fs::read_dir(&base_path).await.unwrap();
    let entry = streams_dir_entries.next_entry().await.unwrap();
    assert!(entry.is_some());
    let entry = entry.unwrap();
    let metadata = entry.metadata().await.unwrap();
    assert!(metadata.is_dir());
    assert_eq!(entry.file_name().into_string().unwrap(), stream_id.to_string());

    let mut stream_dir_entries = fs::read_dir(&stream_path).await.unwrap();
    while let Some(entry) = stream_dir_entries.next_entry().await.unwrap_or(None) {
        let metadata = entry.metadata().await.unwrap();
        if metadata.is_dir() {
            assert_eq!(entry.file_name().into_string().unwrap(), topics_directory);
        } else {
            assert!(metadata.is_file());
            assert_eq!(entry.file_name().into_string().unwrap(), STREAM_INFO);
        }
    }
}