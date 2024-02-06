use crate::streaming::common::test_setup::TestSetup;
use bytes::Bytes;
use iggy::messages::send_messages;
use iggy::models::header::{HeaderKey, HeaderValue};
use server::configs::system::{PartitionConfig, SystemConfig};
use server::streaming::partitions::partition::Partition;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[tokio::test]
async fn should_persist_messages_and_then_load_them_from_disk() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let messages_count = 1000;
    let config = Arc::new(SystemConfig {
        path: setup.config.path.to_string(),
        partition: PartitionConfig {
            messages_required_to_save: messages_count,
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

    let mut messages = Vec::with_capacity(messages_count as usize);
    let mut appended_messages = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let id = i as u128;
        let payload = Bytes::from(format!("message {}", i));
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("key_1").unwrap(),
            HeaderValue::from_str("Value 1").unwrap(),
        );
        headers.insert(
            HeaderKey::new("key 2").unwrap(),
            HeaderValue::from_bool(true).unwrap(),
        );
        headers.insert(
            HeaderKey::new("key-3").unwrap(),
            HeaderValue::from_uint64(123456).unwrap(),
        );
        let appended_message = send_messages::Message {
            id,
            length: payload.len() as u32,
            payload: payload.clone(),
            headers: Some(headers.clone()),
        };
        let message = send_messages::Message {
            id,
            length: payload.len() as u32,
            payload: payload.clone(),
            headers: Some(headers),
        };
        appended_messages.push(appended_message);
        messages.push(message);
    }

    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    partition.append_messages(messages).await.unwrap();
    assert_eq!(partition.unsaved_messages_count, 0);

    let mut loaded_partition = Partition::create(
        stream_id,
        topic_id,
        partition.partition_id,
        false,
        config.clone(),
        setup.storage.clone(),
        None,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    loaded_partition.load().await.unwrap();
    let loaded_messages = loaded_partition
        .get_messages_by_offset(0, messages_count)
        .await
        .unwrap();
    assert_eq!(loaded_messages.len(), messages_count as usize);
    for i in 1..=messages_count {
        let index = i as usize - 1;
        let loaded_message = &loaded_messages[index];
        let appended_message = &appended_messages[index];
        assert_eq!(loaded_message.get_id(), appended_message.id);
        assert_eq!(
            loaded_message.try_get_headers().unwrap(),
            appended_message.headers
        );
    }
}
