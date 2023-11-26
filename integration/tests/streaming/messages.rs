use crate::streaming::common::test_setup::TestSetup;
use bytes::Bytes;
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy::models::messages::{Message, MessageState};
use iggy::utils::{checksum, timestamp::TimeStamp};
use server::configs::system::{PartitionConfig, SystemConfig};
use server::streaming::partitions::partition::Partition;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

#[tokio::test]
async fn should_persist_messages_and_then_load_them_from_disk() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let messages_count = 1024;
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
    );

    let mut messages = Vec::with_capacity(messages_count as usize);
    let mut appended_messages = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let offset = (i - 1) as u64;
        let state = MessageState::Available;
        let timestamp = TimeStamp::now().to_micros();
        let id = i as u128;
        // thanks to this weird the payload, each message will be 128 bytes (including headers)
        let payload = Bytes::from(format!("fancy_test_message___{:04}", i));
        let checksum = checksum::calculate(&payload);
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
        let appended_message = Message::create(
            offset,
            state,
            timestamp,
            id,
            payload.clone(),
            checksum,
            Some(headers.clone()),
        );
        let message = Message::create(
            offset,
            state,
            timestamp,
            id,
            payload,
            checksum,
            Some(headers),
        );
        appended_messages.push(appended_message);
        messages.push(message);
    }

    let are_all_messages_same_size = messages
        .iter()
        .map(|message| message.get_size_bytes())
        .all(|size| size == messages[0].get_size_bytes());

    assert!(
        are_all_messages_same_size,
        "Not all messages are of size {} bytes",
        messages[0].get_size_bytes()
    );

    let total_messages_size = messages
        .iter()
        .map(|message| message.get_size_bytes())
        .sum::<u32>();

    let default_buf_writer_capacity = 8 * 1024;

    assert_eq!(
        total_messages_size % default_buf_writer_capacity,
        0,
        "Total size of messages should be divisible by default buffer writer capacity \
        (total size: {}, default capacity: {}, single message size: {}, number of messages: {})",
        total_messages_size,
        default_buf_writer_capacity,
        messages[0].get_size_bytes(),
        messages_count
    );

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
