use crate::streaming::common::test_setup::TestSetup;
use bytes::Bytes;
use iggy::bytes_serializable::BytesSerializable;
use iggy::messages::send_messages::Message;
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::timestamp::IggyTimestamp;
use server::configs::system::{PartitionConfig, SystemConfig};
use server::state::system::PartitionState;
use server::streaming::batching::appendable_batch_info::AppendableBatchInfo;
use server::streaming::partitions::partition::Partition;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;

#[tokio::test]
async fn should_persist_messages_and_then_load_them_by_timestamp() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let messages_count = 100;
    let config = Arc::new(SystemConfig {
        path: setup.config.path.to_string(),
        partition: PartitionConfig {
            messages_required_to_save: messages_count,
            enforce_fsync: true,
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
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        IggyTimestamp::now(),
    );

    let mut messages = Vec::with_capacity(messages_count as usize);
    let mut appended_messages = Vec::with_capacity(messages_count as usize);
    let mut messages_two = Vec::with_capacity(messages_count as usize);
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
        let message = Message {
            id,
            length: payload.len() as u32,
            payload: payload.clone(),
            headers: Some(headers),
        };
        messages.push(message);
    }

    for i in (messages_count + 1)..=(messages_count * 2) {
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
        let message = Message {
            id,
            length: payload.len() as u32,
            payload: payload.clone(),
            headers: Some(headers),
        };
        appended_messages.push(message.clone());
        messages_two.push(message);
    }

    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    let appendable_batch_info = AppendableBatchInfo::new(
        messages.iter().map(|msg| msg.get_size_bytes() as u64).sum(),
        partition.partition_id,
    );
    let appendable_batch_info_two = AppendableBatchInfo::new(
        messages_two
            .iter()
            .map(|msg| msg.get_size_bytes() as u64)
            .sum(),
        partition.partition_id,
    );
    partition
        .append_messages(appendable_batch_info, messages)
        .await
        .unwrap();
    let test_timestamp = IggyTimestamp::now();
    partition
        .append_messages(appendable_batch_info_two, messages_two)
        .await
        .unwrap();

    let loaded_messages = partition
        .get_messages_by_timestamp(test_timestamp, messages_count)
        .await
        .unwrap();
    assert_eq!(loaded_messages.len(), messages_count as usize);
    for i in (messages_count + 1)..=(messages_count * 2) {
        let index = (i - messages_count - 1) as usize;
        let loaded_message = &loaded_messages[index];
        let appended_message = &appended_messages[index];
        assert_eq!(loaded_message.id, appended_message.id);
        assert_eq!(loaded_message.payload, appended_message.payload);
        assert!(loaded_message.timestamp >= test_timestamp.as_micros());
        assert_eq!(
            loaded_message
                .headers
                .as_ref()
                .map(|bytes| HashMap::from_bytes(bytes.clone()).unwrap()),
            appended_message.headers
        );
    }
}
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
            enforce_fsync: true,
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
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        IggyTimestamp::now(),
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
        let appended_message = Message {
            id,
            length: payload.len() as u32,
            payload: payload.clone(),
            headers: Some(headers.clone()),
        };
        let message = Message {
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
    let appendable_batch_info = AppendableBatchInfo::new(
        messages.iter().map(|msg| msg.get_size_bytes() as u64).sum(),
        partition.partition_id,
    );
    partition
        .append_messages(appendable_batch_info, messages)
        .await
        .unwrap();
    assert_eq!(partition.unsaved_messages_count, 0);

    let now = IggyTimestamp::now();
    let mut loaded_partition = Partition::create(
        stream_id,
        topic_id,
        partition.partition_id,
        false,
        config.clone(),
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
    let loaded_messages = loaded_partition
        .get_messages_by_offset(0, messages_count)
        .await
        .unwrap();
    assert_eq!(loaded_messages.len(), messages_count as usize);
    for i in 1..=messages_count {
        let index = i as usize - 1;
        let loaded_message = &loaded_messages[index];
        let appended_message = &appended_messages[index];
        assert_eq!(loaded_message.id, appended_message.id);
        assert_eq!(loaded_message.payload, appended_message.payload);
        assert_eq!(
            loaded_message
                .headers
                .as_ref()
                .map(|bytes| HashMap::from_bytes(bytes.clone()).unwrap()),
            appended_message.headers
        );
    }
}
