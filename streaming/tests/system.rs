mod common;

use tokio::fs;
use streaming::system::System;
use crate::common::TestSetup;

#[tokio::test]
async fn should_initialize_system_and_base_directories() {
    let setup = TestSetup::init().await;
    let mut system = System::create(setup.config.clone());
    
    system.init().await.unwrap();

    let mut dir_entries = fs::read_dir(&setup.config.path).await.unwrap();
    let entry= dir_entries.next_entry().await.unwrap();
    assert!(entry.is_some());
    let entry = entry.unwrap();
    let metadata = entry.metadata().await.unwrap();
    assert!(metadata.is_dir());
    assert_eq!(entry.file_name().into_string().unwrap(), setup.config.stream.path);
}

#[tokio::test]
async fn should_create_and_persist_stream() {
    let setup = TestSetup::init().await;
    let mut system = System::create(setup.config.clone());
    let stream_id = 1;
    let stream_name = "test";
    system.init().await.unwrap();
    
    system.create_stream(stream_id, stream_name).await.unwrap();
    
    assert_persisted_stream(&system.streams_path, stream_id).await;
}

#[tokio::test]
async fn should_delete_persisted_stream() {
    let setup = TestSetup::init().await;
    let mut system = System::create(setup.config.clone());
    let stream_id = 1;
    let stream_name = "test";
    system.init().await.unwrap();
    system.create_stream(stream_id, stream_name).await.unwrap();
    assert_persisted_stream(&system.streams_path, stream_id).await;
    
    system.delete_stream(stream_id).await.unwrap();

    let mut dir_entries = fs::read_dir(system.streams_path).await.unwrap();
    let entry= dir_entries.next_entry().await.unwrap();
    assert!(entry.is_none());
}

async fn assert_persisted_stream(streams_path: &str, stream_id: u32) {
    let mut dir_entries = fs::read_dir(streams_path).await.unwrap();
    let entry= dir_entries.next_entry().await.unwrap();
    assert!(entry.is_some());
    let entry = entry.unwrap();
    let metadata = entry.metadata().await.unwrap();
    assert!(metadata.is_dir());
    assert_eq!(entry.file_name().into_string().unwrap(), stream_id.to_string());
}