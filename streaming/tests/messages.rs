mod common;

use crate::common::TestSetup;
use std::sync::Arc;
use streaming::config::PartitionConfig;
use streaming::message::Message;
use streaming::partitions::partition::Partition;
use streaming::utils::timestamp;

#[tokio::test]
async fn should_persist_messages_and_then_load_them_from_disk() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let messages_count = 1000;
    let config = Arc::new(PartitionConfig {
        messages_required_to_save: messages_count,
        ..Default::default()
    });
    let mut partition = Partition::create(
        stream_id,
        topic_id,
        partition_id,
        &setup.path,
        true,
        config.clone(),
    );

    let mut messages = Vec::with_capacity(messages_count as usize);
    let mut appended_messages = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let offset = (i - 1) as u64;
        let timestamp = timestamp::get();
        let id = i as u128;
        let payload = format!("message {}", i).as_bytes().to_vec();
        let message = Message::create(offset, timestamp, id, payload);
        appended_messages.push(message.clone());
        messages.push(message);
    }

    partition.persist().await.unwrap();
    partition.append_messages(messages, false).await.unwrap();
    assert_eq!(partition.unsaved_messages_count, 0);

    let mut loaded_partition = Partition::empty(
        stream_id,
        topic_id,
        partition.id,
        &setup.path,
        config.clone(),
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
        assert_eq!(loaded_message.timestamp, appended_message.timestamp);
        assert_eq!(loaded_message.id, appended_message.id);
        assert_eq!(loaded_message.length, appended_message.length);
        assert_eq!(loaded_message.payload, appended_message.payload);
    }
}
