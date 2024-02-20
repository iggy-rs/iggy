use crate::streaming::common::test_setup::TestSetup;
use iggy::locking::IggySharedMutFn;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::messages::send_messages;
use iggy::messages::send_messages::Partitioning;
use iggy::models::messages::Message;
use iggy::utils::byte_size::IggyByteSize;
use server::configs::resource_quota::MemoryResourceQuota;
use server::configs::system::{CacheConfig, SystemConfig};
use server::streaming::polling_consumer::PollingConsumer;
use server::streaming::topics::topic::Topic;
use server::streaming::utils::hash;
use std::collections::HashMap;
use std::str::{from_utf8, FromStr};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[tokio::test]
async fn given_disabled_cache_all_messages_should_be_polled() {
    assert_polling_messages(
        CacheConfig {
            enabled: false,
            ..Default::default()
        },
        false,
    )
    .await;
}

#[tokio::test]
async fn given_enabled_cache_with_enough_capacity_all_messages_should_be_polled() {
    assert_polling_messages(
        CacheConfig {
            enabled: true,
            size: MemoryResourceQuota::Bytes(IggyByteSize::from(100_000_000)),
        },
        true,
    )
    .await;
}

#[tokio::test]
async fn given_enabled_cache_without_enough_capacity_all_messages_should_be_polled() {
    assert_polling_messages(
        CacheConfig {
            enabled: true,
            size: MemoryResourceQuota::Bytes(IggyByteSize::from(100_000)),
        },
        true,
    )
    .await;
}

async fn assert_polling_messages(cache: CacheConfig, expect_enabled_cache: bool) {
    let messages_count = 1000;
    let payload_size_bytes = 1000;
    let config = SystemConfig {
        cache,
        ..Default::default()
    };
    let setup = TestSetup::init_with_config(config).await;
    let topic = init_topic(&setup, 1).await;
    let partition_id = 1;
    let partitioning = Partitioning::partition_id(partition_id);
    let messages = (0..messages_count)
        .map(|id| {
            get_message(format!("{}:{}", id + 1, create_payload(payload_size_bytes)).as_str())
        })
        .collect::<Vec<_>>();
    let mut sent_messages = Vec::new();
    for message in &messages {
        sent_messages.push(get_message(from_utf8(&message.payload).unwrap()))
    }
    topic
        .append_messages(&partitioning, messages)
        .await
        .unwrap();

    let consumer = PollingConsumer::Consumer(1, partition_id);
    let polled_messages = topic
        .get_messages(
            consumer,
            partition_id,
            PollingStrategy::offset(0),
            messages_count,
        )
        .await
        .unwrap();

    assert_eq!(polled_messages.messages.len(), messages_count as usize);
    let partition = topic.get_partition(partition_id).unwrap();
    let partition = partition.read().await;
    assert_eq!(partition.cache.is_some(), expect_enabled_cache);
    if expect_enabled_cache {
        assert!(partition.cache.as_ref().unwrap().current_size() > 0);
    }
    for (index, polled_message) in polled_messages.messages.iter().enumerate() {
        let sent_message = sent_messages.get(index).unwrap();
        assert_eq!(sent_message.payload, polled_message.payload);
        let polled_payload_str = from_utf8(&polled_message.payload).unwrap();
        assert!(polled_payload_str.starts_with(&format!("{}:", index + 1)));
    }
}

#[tokio::test]
async fn given_key_none_messages_should_be_appended_to_the_next_partition_using_round_robin() {
    let setup = TestSetup::init().await;
    let partitions_count = 3;
    let messages_per_partition_count = 10;
    let topic = init_topic(&setup, partitions_count).await;
    let partitioning = Partitioning::balanced();
    for i in 1..=partitions_count * messages_per_partition_count {
        let payload = get_payload(i);
        topic
            .append_messages(&partitioning, vec![get_message(&payload)])
            .await
            .unwrap();
    }
    for i in 1..=partitions_count {
        assert_messages(&topic, i, messages_per_partition_count).await;
    }
}

#[tokio::test]
async fn given_key_partition_id_messages_should_be_appended_to_the_chosen_partition() {
    let setup = TestSetup::init().await;
    let partition_id = 1;
    let partitions_count = 3;
    let messages_per_partition_count = 10;
    let topic = init_topic(&setup, partitions_count).await;
    let partitioning = Partitioning::partition_id(partition_id);
    for i in 1..=partitions_count * messages_per_partition_count {
        let payload = get_payload(i);
        topic
            .append_messages(&partitioning, vec![get_message(&payload)])
            .await
            .unwrap();
    }

    for i in 1..=partitions_count {
        if i == partition_id {
            assert_messages(&topic, i, messages_per_partition_count * partitions_count).await;
            continue;
        }
        assert_messages(&topic, i, 0).await;
    }
}

#[tokio::test]
async fn given_key_messages_key_messages_should_be_appended_to_the_calculated_partition() {
    let setup = TestSetup::init().await;
    let partitions_count = 3;
    let messages_count = 10;
    let topic = init_topic(&setup, partitions_count).await;
    for entity_id in 1..=partitions_count * messages_count {
        let payload = get_payload(entity_id);
        let partitioning = Partitioning::messages_key_u32(entity_id);
        topic
            .append_messages(&partitioning, vec![get_message(&payload)])
            .await
            .unwrap();
    }

    let mut messages_count_per_partition = HashMap::new();
    for entity_id in 1..=partitions_count * messages_count {
        let key = Partitioning::messages_key_u32(entity_id);
        let hash = hash::calculate_32(&key.value);
        let mut partition_id = hash % partitions_count;
        if partition_id == 0 {
            partition_id = partitions_count;
        }

        messages_count_per_partition
            .entry(partition_id)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    for (partition_id, expected_messages) in messages_count_per_partition {
        assert_messages(&topic, partition_id, expected_messages).await;
    }
}

fn get_payload(id: u32) -> String {
    format!("message-{}", id)
}

async fn assert_messages(topic: &Topic, partition_id: u32, expected_messages: u32) {
    let consumer = PollingConsumer::Consumer(0, partition_id);
    let polled_messages = topic
        .get_messages(consumer, partition_id, PollingStrategy::offset(0), 1000)
        .await
        .unwrap();
    assert_eq!(polled_messages.messages.len() as u32, expected_messages);
}

async fn init_topic(setup: &TestSetup, partitions_count: u32) -> Topic {
    let stream_id = 1;
    setup.create_topics_directory(stream_id).await;
    let id = 2;
    let name = "test";
    let topic = Topic::create(
        stream_id,
        id,
        name,
        partitions_count,
        setup.config.clone(),
        setup.storage.clone(),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        None,
        None,
        1,
    )
    .unwrap();
    topic.persist().await.unwrap();
    topic
}

fn get_message(payload: &str) -> Message {
    Message::from_message(&send_messages::Message::from_str(payload).unwrap())
}

fn create_payload(size: u32) -> String {
    let mut payload = String::with_capacity(size as usize);
    for i in 0..size {
        let char = (i % 26 + 97) as u8 as char;
        payload.push(char);
    }

    payload
}
