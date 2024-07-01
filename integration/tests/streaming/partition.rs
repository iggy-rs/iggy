use crate::streaming::common::test_setup::TestSetup;
use crate::streaming::create_messages;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::timestamp::IggyTimestamp;
use server::state::system::PartitionState;
use server::streaming::batching::appendable_batch_info::AppendableBatchInfo;
use server::streaming::partitions::partition::Partition;
use server::streaming::segments::segment::{INDEX_EXTENSION, LOG_EXTENSION, TIME_INDEX_EXTENSION};
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;
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
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        );

        partition.persist().await.unwrap();

        assert_persisted_partition(&partition.partition_path, with_segment).await;
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
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        );
        partition.persist().await.unwrap();
        assert_persisted_partition(&partition.partition_path, with_segment).await;

        let now = IggyTimestamp::now();
        let mut loaded_partition = Partition::create(
            stream_id,
            topic_id,
            partition.partition_id,
            false,
            setup.config.clone(),
            setup.storage.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            now,
        );
        let partition_state = PartitionState {
            id: partition.partition_id,
            created_at: now,
        };
        loaded_partition.load(partition_state).await.unwrap();

        assert_eq!(loaded_partition.stream_id, partition.stream_id);
        assert_eq!(loaded_partition.partition_id, partition.partition_id);
        assert_eq!(loaded_partition.partition_path, partition.partition_path);
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
        assert_eq!(loaded_partition.cache.is_some(), partition.cache.is_some());
        assert_eq!(
            loaded_partition.cache.unwrap().is_empty(),
            partition.cache.unwrap().is_empty()
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
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        );
        partition.persist().await.unwrap();
        assert_persisted_partition(&partition.partition_path, with_segment).await;

        partition.delete().await.unwrap();

        assert!(fs::metadata(&partition.partition_path).await.is_err());
    }
}

#[tokio::test]
async fn should_purge_existing_partition_on_disk() {
    let setup = TestSetup::init().await;
    let with_segment = true;
    let stream_id = 1;
    let topic_id = 2;
    setup.create_partitions_directory(stream_id, topic_id).await;
    let partition_ids = get_partition_ids();
    for partition_id in partition_ids {
        let mut partition = Partition::create(
            stream_id,
            topic_id,
            partition_id,
            with_segment,
            setup.config.clone(),
            setup.storage.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        );
        partition.persist().await.unwrap();
        assert_persisted_partition(&partition.partition_path, with_segment).await;
        let messages = create_messages();
        let messages_count = messages.len();
        let appendable_batch_info = AppendableBatchInfo::new(
            messages.iter().map(|msg| msg.get_size_bytes() as u64).sum(),
            partition.partition_id,
        );
        partition
            .append_messages(appendable_batch_info, messages)
            .await
            .unwrap();
        let loaded_messages = partition.get_messages_by_offset(0, 100).await.unwrap();
        assert_eq!(loaded_messages.len(), messages_count);
        partition.purge().await.unwrap();
        assert_eq!(partition.current_offset, 0);
        assert_eq!(partition.unsaved_messages_count, 0);
        assert!(!partition.should_increment_offset);
        let loaded_messages = partition.get_messages_by_offset(0, 100).await.unwrap();
        assert!(loaded_messages.is_empty());
    }
}

async fn assert_persisted_partition(partition_path: &str, with_segment: bool) {
    assert!(fs::metadata(&partition_path).await.is_ok());

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
