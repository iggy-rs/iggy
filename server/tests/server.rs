mod common;

use crate::common::TestServer;
use sdk::quic::client::Client;
use sdk::quic::config::Config;
use shared::messages::poll_messages::Kind::Offset;
use shared::messages::poll_messages::{Format, PollMessages};
use shared::messages::send_messages::{KeyKind, Message, SendMessages};
use shared::streams::create_stream::CreateStream;
use shared::streams::get_streams::GetStreams;
use shared::system::ping::Ping;
use shared::topics::create_topic::CreateTopic;
use shared::topics::get_topics::GetTopics;

#[tokio::test]
async fn stream_should_be_created_and_messages_should_be_appended_to_the_partition() {
    let test_server = TestServer::default();
    test_server.start();
    let client = Client::create(Config::default()).unwrap();
    let client = client.connect().await.unwrap();
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;

    // 1. Ping server
    let ping = Ping {};
    client.ping(&ping).await.unwrap();

    // 2. Ensure that streams do not exist
    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert!(streams.is_empty());

    // 3. Create the stream
    let create_stream = CreateStream {
        stream_id,
        name: "test".to_string(),
    };
    client.create_stream(&create_stream).await.unwrap();

    // 4. Get streams and validate that created stream exists
    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert_eq!(streams.len(), 1);
    let stream = streams.get(0).unwrap();
    assert_eq!(stream.id, create_stream.stream_id);
    assert_eq!(stream.name, create_stream.name);

    // 5. Create the topic
    let create_topic = CreateTopic {
        stream_id,
        topic_id,
        partitions_count: 2,
        name: "test".to_string(),
    };
    client.create_topic(&create_topic).await.unwrap();

    // 6. Get topics and validate that created topic exists
    let get_topics = GetTopics { stream_id };
    let topics = client.get_topics(&get_topics).await.unwrap();
    assert_eq!(topics.len(), 1);
    let topic = topics.get(0).unwrap();
    assert_eq!(topic.id, create_topic.topic_id);
    assert_eq!(topic.name, create_topic.name);
    assert_eq!(topic.partitions, create_topic.partitions_count);

    // 7. Send messages to the specific topic and partition
    let messages_count = 1000u32;
    let mut messages = Vec::new();
    for offset in 0..messages_count {
        let payload = get_message_payload(offset as u64);
        messages.push(Message {
            length: payload.len() as u32,
            payload,
        });
    }

    let send_messages = SendMessages {
        stream_id: create_stream.stream_id,
        topic_id: create_topic.topic_id,
        key_kind: KeyKind::PartitionId,
        key_value: partition_id,
        messages_count,
        messages,
    };
    client.send_messages(&send_messages).await.unwrap();

    // 8. Poll messages from the specific partition in topic
    let mut poll_messages = PollMessages {
        consumer_id: 0,
        stream_id,
        topic_id,
        partition_id,
        kind: Offset,
        value: 0,
        count: messages_count,
        auto_commit: false,
        format: Format::None,
    };

    let messages = client.poll_messages(&poll_messages).await.unwrap();
    assert_eq!(messages.len() as u32, messages_count);
    for i in 0..messages_count {
        let offset = i as u64;
        let message = messages.get(i as usize).unwrap();
        assert_message(message, offset);
    }

    // 9. Messages should be also polled in the smaller batches
    let batches_count = 10;
    let batch_size = messages_count / batches_count;

    for i in 0..batches_count {
        let start_offset = (i * batch_size) as u64;
        poll_messages.count = batch_size;
        poll_messages.value = start_offset;
        let messages = client.poll_messages(&poll_messages).await.unwrap();
        assert_eq!(messages.len() as u32, batch_size);
        for i in 0..batch_size as u64 {
            let offset = start_offset + i;
            let message = messages.get(i as usize).unwrap();
            assert_message(message, offset);
        }
    }

    // 10. Ensure that messages do not exist in the second partition in the same topic
    poll_messages.partition_id += 1;
    let messages = client.poll_messages(&poll_messages).await.unwrap();
    assert!(messages.is_empty());

    test_server.stop();
}

fn assert_message(message: &sdk::message::Message, offset: u64) {
    let expected_payload = get_message_payload(offset);
    assert!(message.timestamp > 0);
    assert_eq!(message.offset, offset);
    assert_eq!(message.length, expected_payload.len() as u32);
    assert_eq!(message.payload, expected_payload);
}

fn get_message_payload(offset: u64) -> Vec<u8> {
    format!("message {}", offset).as_bytes().to_vec()
}
