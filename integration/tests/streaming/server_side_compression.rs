use crate::streaming::common::test_setup::TestSetup;
use crate::streaming::{create_message, create_random_payload};
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use serial_test::parallel;
use server::configs::system::{PartitionConfig, SystemConfig};
use server::streaming::partitions::partition::Partition;
use std::sync::Arc;

#[tokio::test]
#[parallel]
pub async fn should_persist_and_load_gzip_compressed_messages() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let messages_count: u64 = 100;
    let config = Arc::new(SystemConfig {
        path: setup.config.path.to_string(),
        partition: PartitionConfig {
            messages_required_to_save: messages_count as u32,
            ..Default::default()
        },
        ..Default::default()
    });
    let mut partition = Partition::create(
        stream_id,
        topic_id,
        partition_id,
        true,
        config.clone(),
        setup.storage.clone(),
        None,
    );
    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    let mut messages = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let message = create_message(i, (i + 69420) as u128, &create_random_payload(5..25));
        messages.push(message);
    }
    partition
        .append_messages(CompressionAlgorithm::Gzip, messages)
        .await
        .unwrap();

    let retrieved_messages = partition
        .get_messages_by_offset(0, messages_count as u32)
        .await
        .unwrap();
    assert_eq!(retrieved_messages.len(), messages_count as usize);
    assert_eq!(partition.unsaved_messages_count, 0);
}
#[tokio::test]
#[parallel]
pub async fn should_persist_and_load_lz4_compressed_messages() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let messages_count: u64 = 150;
    let config = Arc::new(SystemConfig {
        path: setup.config.path.to_string(),
        partition: PartitionConfig {
            messages_required_to_save: messages_count as u32,
            ..Default::default()
        },
        ..Default::default()
    });
    let mut partition = Partition::create(
        stream_id,
        topic_id,
        partition_id,
        true,
        config.clone(),
        setup.storage.clone(),
        None,
    );
    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    let mut messages = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let message = create_message(i, (i + 69420) as u128, &create_random_payload(25..50));
        messages.push(message);
    }
    partition
        .append_messages(CompressionAlgorithm::Lz4, messages)
        .await
        .unwrap();

    let retrieved_messages = partition
        .get_messages_by_offset(0, messages_count as u32)
        .await
        .unwrap();
    assert_eq!(retrieved_messages.len(), messages_count as usize);
    assert_eq!(partition.unsaved_messages_count, 0);
}
#[tokio::test]
#[parallel]
pub async fn should_persist_and_load_zstd_compressed_messages() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let messages_count: u64 = 125;
    let config = Arc::new(SystemConfig {
        path: setup.config.path.to_string(),
        partition: PartitionConfig {
            messages_required_to_save: messages_count as u32,
            ..Default::default()
        },
        ..Default::default()
    });
    let mut partition = Partition::create(
        stream_id,
        topic_id,
        partition_id,
        true,
        config.clone(),
        setup.storage.clone(),
        None,
    );
    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    let mut messages = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let message = create_message(i, (i + 69420) as u128, &create_random_payload(30..60));
        messages.push(message);
    }
    partition
        .append_messages(CompressionAlgorithm::Zstd, messages)
        .await
        .unwrap();

    let retrieved_messages = partition
        .get_messages_by_offset(0, messages_count as u32)
        .await
        .unwrap();
    assert_eq!(retrieved_messages.len(), messages_count as usize);
    assert_eq!(partition.unsaved_messages_count, 0);
}
