mod common;

use crate::common::TestSetup;
use ringbuffer::RingBuffer;
use streaming::partitions::partition::Partition;
use streaming::segments::segment::{INDEX_EXTENSION, LOG_EXTENSION, TIME_INDEX_EXTENSION};
use tokio::fs;

#[tokio::test]
async fn should_persist_partition_with_segment() {
    let setup = TestSetup::init().await;
    let with_segment = true;
    let stream_id = 1;
    let topic_id = 2;
    setup.create_partitions_directory(stream_id, topic_id).await;
    let partition_ids = get_partition_ids();
    for partition_id in partition_ids {
        let partition = Partition::create(
            stream_id,
            topic_id,
            partition_id,
            with_segment,
            setup.config.clone(),
            setup.storage.clone(),
            None,
        );

        partition.persist().await.unwrap();

        assert_persisted_partition(&partition.path, with_segment).await;
    }
}

#[tokio::test]
async fn should_load_existing_partition_from_disk() {
    let setup = TestSetup::init().await;
    let with_segment = true;
    let stream_id = 1;
    let topic_id = 2;
    setup.create_partitions_directory(stream_id, topic_id).await;
    let partition_ids = get_partition_ids();
    for partition_id in partition_ids {
        let partition = Partition::create(
            stream_id,
            topic_id,
            partition_id,
            with_segment,
            setup.config.clone(),
            setup.storage.clone(),
            None,
        );
        partition.persist().await.unwrap();
        assert_persisted_partition(&partition.path, with_segment).await;

        let mut loaded_partition = Partition::empty(
            stream_id,
            topic_id,
            partition.partition_id,
            setup.config.clone(),
            setup.storage.clone(),
        );
        loaded_partition.load().await.unwrap();

        assert_eq!(loaded_partition.stream_id, partition.stream_id);
        assert_eq!(loaded_partition.partition_id, partition.partition_id);
        assert_eq!(loaded_partition.path, partition.path);
        assert_eq!(loaded_partition.offsets_path, partition.offsets_path);
        assert_eq!(loaded_partition.current_offset, partition.current_offset);
        assert_eq!(
            loaded_partition.unsaved_messages_count,
            partition.unsaved_messages_count
        );
        assert_eq!(
            loaded_partition.get_segments().len(),
            partition.get_segments().len()
        );
        assert_eq!(
            loaded_partition.should_increment_offset,
            partition.should_increment_offset
        );
        assert_eq!(
            loaded_partition.messages.is_some(),
            partition.messages.is_some()
        );
        assert_eq!(
            loaded_partition.messages.unwrap().is_empty(),
            partition.messages.unwrap().is_empty()
        );
    }
}

#[tokio::test]
async fn should_delete_existing_partition_from_disk() {
    let setup = TestSetup::init().await;
    let with_segment = true;
    let stream_id = 1;
    let topic_id = 2;
    setup.create_partitions_directory(stream_id, topic_id).await;
    let partition_ids = get_partition_ids();
    for partition_id in partition_ids {
        let partition = Partition::create(
            stream_id,
            topic_id,
            partition_id,
            with_segment,
            setup.config.clone(),
            setup.storage.clone(),
            None,
        );
        partition.persist().await.unwrap();
        assert_persisted_partition(&partition.path, with_segment).await;

        partition.delete().await.unwrap();

        assert!(fs::metadata(&partition.path).await.is_err());
    }
}

async fn assert_persisted_partition(partition_path: &str, with_segment: bool) {
    let offsets_path = format!("{}/offsets", partition_path);
    let consumer_offsets_path = format!("{}/consumers", offsets_path);

    assert!(fs::metadata(&partition_path).await.is_ok());
    assert!(fs::metadata(&offsets_path).await.is_ok());
    assert!(fs::metadata(&consumer_offsets_path).await.is_ok());

    if with_segment {
        let start_offset = 0u64;
        let segment_path = format!("{}/{:0>20}", partition_path, start_offset);
        let log_path = format!("{}.{}", segment_path, LOG_EXTENSION);
        let index_path = format!("{}.{}", segment_path, INDEX_EXTENSION);
        let time_index_path = format!("{}.{}", segment_path, TIME_INDEX_EXTENSION);
        assert!(fs::metadata(&log_path).await.is_ok());
        assert!(fs::metadata(&index_path).await.is_ok());
        assert!(fs::metadata(&time_index_path).await.is_ok());
    }
}

fn get_partition_ids() -> Vec<u32> {
    vec![1, 2, 3, 5, 10, 100, 1000, 99999]
}
