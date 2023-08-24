mod common;

use crate::common::TestSetup;
use std::sync::Arc;
use streaming::segments::segment;
use streaming::segments::segment::{INDEX_EXTENSION, LOG_EXTENSION, TIME_INDEX_EXTENSION};
use streaming::storage::SystemStorage;
use tokio::fs;

#[tokio::test]
async fn should_persist_segment() {
    let setup = TestSetup::init().await;
    let storage = Arc::new(SystemStorage::default());
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offsets = get_start_offsets();
    for start_offset in start_offsets {
        let partition_path = &setup.path;
        let segment = segment::Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            partition_path,
            setup.config.clone(),
            storage.clone(),
            None,
        );

        segment.persist().await.unwrap();
        assert_persisted_segment(partition_path, start_offset).await;
    }
}

#[tokio::test]
async fn should_load_existing_segment_from_disk() {
    let setup = TestSetup::init().await;
    let storage = Arc::new(SystemStorage::default());
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offsets = get_start_offsets();
    for start_offset in start_offsets {
        let partition_path = &setup.path;
        let segment = segment::Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            partition_path,
            setup.config.clone(),
            storage.clone(),
            None,
        );
        segment.persist().await.unwrap();
        assert_persisted_segment(partition_path, start_offset).await;

        let mut loaded_segment = segment::Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            partition_path,
            setup.config.clone(),
            storage.clone(),
            None,
        );
        loaded_segment.load().await.unwrap();

        assert_eq!(loaded_segment.partition_id, segment.partition_id);
        assert_eq!(loaded_segment.start_offset, segment.start_offset);
        assert_eq!(loaded_segment.current_offset, segment.current_offset);
        assert_eq!(loaded_segment.end_offset, segment.end_offset);
        assert_eq!(
            loaded_segment.current_size_bytes,
            segment.current_size_bytes
        );
        assert_eq!(loaded_segment.is_closed, segment.is_closed);
        assert_eq!(loaded_segment.log_path, segment.log_path);
        assert_eq!(loaded_segment.index_path, segment.index_path);
        assert_eq!(loaded_segment.time_index_path, segment.time_index_path);
    }
}

async fn assert_persisted_segment(partition_path: &str, start_offset: u64) {
    let segment_path = format!("{}/{:0>20}", partition_path, start_offset);
    let log_path = format!("{}.{}", segment_path, LOG_EXTENSION);
    let index_path = format!("{}.{}", segment_path, INDEX_EXTENSION);
    let time_index_path = format!("{}.{}", segment_path, TIME_INDEX_EXTENSION);
    assert!(fs::metadata(&log_path).await.is_ok());
    assert!(fs::metadata(&index_path).await.is_ok());
    assert!(fs::metadata(&time_index_path).await.is_ok());
}

fn get_start_offsets() -> Vec<u64> {
    vec![
        0, 1, 2, 9, 10, 99, 100, 110, 200, 1000, 1234, 12345, 100000, 9999999,
    ]
}
