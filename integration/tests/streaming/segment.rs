use crate::streaming::common::test_setup::TestSetup;
use bytes::Bytes;
use iggy::models::messages::{Message, MessageState};
use iggy::utils::{checksum, timestamp::TimeStamp};
use server::streaming::segments::segment;
use server::streaming::segments::segment::{INDEX_EXTENSION, LOG_EXTENSION, TIME_INDEX_EXTENSION};
use std::sync::Arc;
use tokio::fs;

#[tokio::test]
async fn should_persist_segment() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offsets = get_start_offsets();
    for start_offset in start_offsets {
        let segment = segment::Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            setup.config.clone(),
            setup.storage.clone(),
            None,
        );

        setup
            .create_partition_directory(stream_id, topic_id, partition_id)
            .await;
        segment.persist().await.unwrap();
        assert_persisted_segment(
            &setup
                .config
                .get_partition_path(stream_id, topic_id, partition_id),
            start_offset,
        )
        .await;
    }
}

#[tokio::test]
async fn should_load_existing_segment_from_disk() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offsets = get_start_offsets();
    for start_offset in start_offsets {
        let segment = segment::Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            setup.config.clone(),
            setup.storage.clone(),
            None,
        );
        setup
            .create_partition_directory(stream_id, topic_id, partition_id)
            .await;
        segment.persist().await.unwrap();
        assert_persisted_segment(
            &setup
                .config
                .get_partition_path(stream_id, topic_id, partition_id),
            start_offset,
        )
        .await;

        let mut loaded_segment = segment::Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            setup.config.clone(),
            setup.storage.clone(),
            None,
        );
        loaded_segment.load().await.unwrap();
        let loaded_messages = loaded_segment.get_messages(0, 10).await.unwrap();

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
        assert!(loaded_messages.is_empty());
    }
}

#[tokio::test]
async fn should_persist_and_load_segment_with_messages() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offset = 0;
    let mut segment = segment::Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        setup.storage.clone(),
        None,
    );

    setup
        .create_partition_directory(stream_id, topic_id, partition_id)
        .await;
    segment.persist().await.unwrap();
    assert_persisted_segment(
        &setup
            .config
            .get_partition_path(stream_id, topic_id, partition_id),
        start_offset,
    )
    .await;

    // 128 messages of size 64 bytes (including headers) means 8192 bytes, which is the default
    // BufWriter capacity - this means that the segment should be flushed to disk after appending
    // all messages.
    let messages_count = 128;
    let mut total_messages_size = 0;
    let create_test_message =
        |index| create_message(index, "fancy_test_payload_", TimeStamp::now().to_micros());
    let expected_message_size = create_test_message(0).get_size_bytes();

    for i in 0..messages_count {
        let message = create_message(i, "fancy_test_payload_", TimeStamp::now().to_micros());
        let message_size = message.get_size_bytes();
        total_messages_size += message_size;
        assert_eq!(
            message_size, expected_message_size,
            "Message size does not match the expected size"
        );
        segment.append_messages(&[Arc::new(message)]).await.unwrap();
    }

    let default_buf_writer_capacity = 8192;
    assert_eq!(
        total_messages_size % default_buf_writer_capacity,
        0,
        "Total size of messages should be divisible by default buffer writer capacity \
        (total size: {}, default capacity: {}, single message size: {}, number of messages: {})",
        total_messages_size,
        default_buf_writer_capacity,
        expected_message_size,
        messages_count
    );

    segment
        .persist_messages(setup.storage.segment.clone())
        .await
        .unwrap();

    let mut loaded_segment = segment::Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        setup.storage.clone(),
        None,
    );
    loaded_segment.load().await.unwrap();
    let messages = loaded_segment
        .get_messages(0, messages_count as u32)
        .await
        .unwrap();
    assert_eq!(messages.len(), messages_count as usize);
}

#[tokio::test]
async fn given_all_expired_messages_segment_should_be_expired() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offset = 0;
    let message_expiry = 10;
    let mut segment = segment::Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        setup.storage.clone(),
        Some(message_expiry),
    );

    setup
        .create_partition_directory(stream_id, topic_id, partition_id)
        .await;
    segment.persist().await.unwrap();
    assert_persisted_segment(
        &setup
            .config
            .get_partition_path(stream_id, topic_id, partition_id),
        start_offset,
    )
    .await;
    let messages_count = 10;
    let now = TimeStamp::now().to_micros();
    let message_expiry = message_expiry as u64;
    let mut expired_timestamp = now - (1000 * 2 * message_expiry);
    for i in 0..messages_count {
        let message = create_message(i, "test", expired_timestamp);
        expired_timestamp += 1;
        segment.append_messages(&[Arc::new(message)]).await.unwrap();
    }

    segment
        .persist_messages(setup.storage.segment.clone())
        .await
        .unwrap();

    let is_expired = segment.is_expired(now).await;
    assert!(is_expired);
}

#[tokio::test]
async fn given_at_least_one_not_expired_message_segment_should_not_be_expired() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offset = 0;
    let message_expiry = 10;
    let mut segment = segment::Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        setup.storage.clone(),
        Some(message_expiry),
    );

    setup
        .create_partition_directory(stream_id, topic_id, partition_id)
        .await;
    segment.persist().await.unwrap();
    assert_persisted_segment(
        &setup
            .config
            .get_partition_path(stream_id, topic_id, partition_id),
        start_offset,
    )
    .await;
    let now = TimeStamp::now().to_micros();
    let message_expiry = message_expiry as u64;
    let expired_timestamp = now - (1000 * 2 * message_expiry);
    let not_expired_timestamp = now - (1000 * message_expiry) + 1;
    let expired_message = create_message(0, "test", expired_timestamp);
    let not_expired_message = create_message(1, "test", not_expired_timestamp);

    segment
        .append_messages(&[Arc::new(expired_message)])
        .await
        .unwrap();
    segment
        .append_messages(&[Arc::new(not_expired_message)])
        .await
        .unwrap();
    segment
        .persist_messages(setup.storage.segment.clone())
        .await
        .unwrap();

    let is_expired = segment.is_expired(now).await;
    assert!(!is_expired);
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

fn create_message(offset: u64, payload: &str, timestamp: u64) -> Message {
    let payload = Bytes::from(payload.to_string());
    let checksum = checksum::calculate(payload.as_ref());
    Message::create(
        offset,
        MessageState::Available,
        timestamp,
        0,
        payload,
        checksum,
        None,
    )
}

fn get_start_offsets() -> Vec<u64> {
    vec![
        0, 1, 2, 9, 10, 99, 100, 110, 200, 1000, 1234, 12345, 100000, 9999999,
    ]
}
