use crate::streaming::common::test_setup::TestSetup;
use crate::streaming::{create_message, create_random_payload};
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use serial_test::parallel;
use server::configs::system::{PartitionConfig, SystemConfig};
use server::streaming::partitions::partition::Partition;
use std::sync::atomic::AtomicU64;
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
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    let mut messages = Vec::with_capacity(messages_count as usize);
    let mut appended_messages = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let message = create_message(i - 1, (i + 69420) as u128, &create_random_payload(5..25));
        messages.push(message.clone());
        appended_messages.push(message);
    }
    partition
        .append_messages(CompressionAlgorithm::Gzip, messages)
        .await
        .unwrap();

    let loaded_messages = partition
        .get_messages_by_offset(0, messages_count as u32)
        .await
        .unwrap();
    assert_eq!(loaded_messages.len(), messages_count as usize);
    assert_eq!(partition.unsaved_messages_count, 0);
    for (loaded_message, appended_message) in loaded_messages.iter().zip(appended_messages) {
        assert_eq!(loaded_message.offset, appended_message.offset);
        assert_eq!(loaded_message.state, appended_message.state);
        assert_eq!(loaded_message.timestamp, appended_message.timestamp);
        assert_eq!(loaded_message.id, appended_message.id);
        assert_eq!(loaded_message.checksum, appended_message.checksum);
        assert_eq!(loaded_message.length, appended_message.length);
        assert_eq!(loaded_message.payload, appended_message.payload);
        assert_eq!(loaded_message.headers, appended_message.headers);
    }
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
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    let mut messages = Vec::with_capacity(messages_count as usize);
    let mut appended_messages = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let message = create_message(i - 1, (i + 69420) as u128, &create_random_payload(25..65));
        messages.push(message.clone());
        appended_messages.push(message);
    }
    partition
        .append_messages(CompressionAlgorithm::Lz4, messages)
        .await
        .unwrap();

    let loaded_messages = partition
        .get_messages_by_offset(0, messages_count as u32)
        .await
        .unwrap();
    assert_eq!(loaded_messages.len(), messages_count as usize);
    assert_eq!(partition.unsaved_messages_count, 0);
    for (loaded_message, appended_message) in loaded_messages.iter().zip(appended_messages) {
        assert_eq!(loaded_message.offset, appended_message.offset);
        assert_eq!(loaded_message.state, appended_message.state);
        assert_eq!(loaded_message.timestamp, appended_message.timestamp);
        assert_eq!(loaded_message.id, appended_message.id);
        assert_eq!(loaded_message.checksum, appended_message.checksum);
        assert_eq!(loaded_message.length, appended_message.length);
        assert_eq!(loaded_message.payload, appended_message.payload);
        assert_eq!(loaded_message.headers, appended_message.headers);
    }
}

#[tokio::test]
#[parallel]
pub async fn should_persist_and_load_zstd_compressed_messages() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let messages_count: u64 = 80;
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
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    let mut messages = Vec::with_capacity(messages_count as usize);
    let mut appended_messages = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let message = create_message(i - 1, (i + 69420) as u128, &create_random_payload(10..35));
        messages.push(message.clone());
        appended_messages.push(message);
    }
    partition
        .append_messages(CompressionAlgorithm::Zstd, messages)
        .await
        .unwrap();

    let loaded_messages = partition
        .get_messages_by_offset(0, messages_count as u32)
        .await
        .unwrap();
    assert_eq!(loaded_messages.len(), messages_count as usize);
    assert_eq!(partition.unsaved_messages_count, 0);
    for (loaded_message, appended_message) in loaded_messages.iter().zip(appended_messages) {
        assert_eq!(loaded_message.offset, appended_message.offset);
        assert_eq!(loaded_message.state, appended_message.state);
        assert_eq!(loaded_message.timestamp, appended_message.timestamp);
        assert_eq!(loaded_message.id, appended_message.id);
        assert_eq!(loaded_message.checksum, appended_message.checksum);
        assert_eq!(loaded_message.length, appended_message.length);
        assert_eq!(loaded_message.payload, appended_message.payload);
        assert_eq!(loaded_message.headers, appended_message.headers);
    }
}
