use crate::common::{ClientFactory, TestServer};
use bytes::Bytes;
use iggy::client::{ConsumerGroupClient, MessageClient, StreamClient, SystemClient, TopicClient};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::get_consumer_groups::GetConsumerGroups;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use iggy::consumer_type::ConsumerType;
use iggy::error::Error;
use iggy::messages::poll_messages::Kind::{Next, Offset};
use iggy::messages::poll_messages::{Format, PollMessages};
use iggy::messages::send_messages::{Key, Message, SendMessages};
use iggy::offsets::get_offset::GetOffset;
use iggy::offsets::store_offset::StoreOffset;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::streams::get_stream::GetStream;
use iggy::streams::get_streams::GetStreams;
use iggy::system::get_clients::GetClients;
use iggy::system::get_me::GetMe;
use iggy::system::get_stats::GetStats;
use iggy::system::ping::Ping;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::topics::get_topic::GetTopic;
use iggy::topics::get_topics::GetTopics;
use tokio::time::sleep;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;
const PARTITIONS_COUNT: u32 = 3;
const CONSUMER_ID: u32 = 1;
const CONSUMER_TYPE: ConsumerType = ConsumerType::Consumer;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const CONSUMER_GROUP_ID: u32 = 1;
const MESSAGES_COUNT: u32 = 1000;

#[allow(dead_code)]
pub async fn run(client_factory: &dyn ClientFactory) {
    let test_server = TestServer::default();
    test_server.start();
    sleep(std::time::Duration::from_secs(1)).await;
    let client = client_factory.create_client().await;
    let client = IggyClient::new(client, IggyClientConfig::default());

    // 1. Ping server
    let ping = Ping {};
    client.ping(&ping).await.unwrap();

    // 2. Ensure that streams do not exist
    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert!(streams.is_empty());

    // 3. Create the stream
    let create_stream = CreateStream {
        stream_id: STREAM_ID,
        name: STREAM_NAME.to_string(),
    };
    client.create_stream(&create_stream).await.unwrap();

    // 4. Get streams and validate that created stream exists
    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert_eq!(streams.len(), 1);
    let stream = streams.get(0).unwrap();
    assert_eq!(stream.id, STREAM_ID);
    assert_eq!(stream.name, STREAM_NAME);
    assert_eq!(stream.topics_count, 0);
    assert_eq!(stream.size_bytes, 0);
    assert_eq!(stream.messages_count, 0);

    // 5. Get stream details
    let stream = client
        .get_stream(&GetStream {
            stream_id: STREAM_ID,
        })
        .await
        .unwrap();
    assert_eq!(stream.id, STREAM_ID);
    assert_eq!(stream.name, STREAM_NAME);
    assert_eq!(stream.topics_count, 0);
    assert!(stream.topics.is_empty());
    assert_eq!(stream.size_bytes, 0);
    assert_eq!(stream.messages_count, 0);

    // 6. Create the topic
    let create_topic = CreateTopic {
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        partitions_count: PARTITIONS_COUNT,
        name: TOPIC_NAME.to_string(),
    };
    client.create_topic(&create_topic).await.unwrap();

    // 7. Get topics and validate that created topic exists
    let topics = client
        .get_topics(&GetTopics {
            stream_id: STREAM_ID,
        })
        .await
        .unwrap();
    assert_eq!(topics.len(), 1);
    let topic = topics.get(0).unwrap();
    assert_eq!(topic.id, TOPIC_ID);
    assert_eq!(topic.name, TOPIC_NAME);
    assert_eq!(topic.partitions_count, PARTITIONS_COUNT);
    assert_eq!(topic.size_bytes, 0);
    assert_eq!(topic.messages_count, 0);

    // 8. Get topic details
    let topic = client
        .get_topic(&GetTopic {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
        })
        .await
        .unwrap();
    assert_eq!(topic.id, TOPIC_ID);
    assert_eq!(topic.name, TOPIC_NAME);
    assert_eq!(topic.partitions_count, PARTITIONS_COUNT);
    assert_eq!(topic.partitions.len(), PARTITIONS_COUNT as usize);
    assert_eq!(topic.size_bytes, 0);
    assert_eq!(topic.messages_count, 0);
    let mut id = 1;
    for topic_partition in topic.partitions {
        assert_eq!(topic_partition.id, id);
        assert_eq!(topic_partition.segments_count, 1);
        assert_eq!(topic_partition.size_bytes, 0);
        assert_eq!(topic_partition.current_offset, 0);
        assert_eq!(topic_partition.messages_count, 0);
        id += 1;
    }

    // 9. Get stream details and validate that created topic exists
    let stream = client
        .get_stream(&GetStream {
            stream_id: STREAM_ID,
        })
        .await
        .unwrap();
    assert_eq!(stream.id, STREAM_ID);
    assert_eq!(stream.name, STREAM_NAME);
    assert_eq!(stream.topics_count, 1);
    assert_eq!(stream.topics.len(), 1);
    assert_eq!(stream.messages_count, 0);
    let stream_topic = stream.topics.get(0).unwrap();
    assert_eq!(stream_topic.id, topic.id);
    assert_eq!(stream_topic.name, topic.name);
    assert_eq!(stream_topic.partitions_count, topic.partitions_count);
    assert_eq!(stream_topic.size_bytes, 0);
    assert_eq!(stream_topic.messages_count, 0);

    // 10. Send messages to the specific topic and partition
    let mut messages = Vec::new();
    for offset in 0..MESSAGES_COUNT {
        let id = (offset + 1) as u128;
        let payload = get_message_payload(offset as u64);
        messages.push(Message {
            id,
            length: payload.len() as u32,
            payload,
        });
    }

    let send_messages = SendMessages {
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        key: Key::partition_id(PARTITION_ID),
        messages_count: MESSAGES_COUNT,
        messages,
    };
    client.send_messages(&send_messages).await.unwrap();

    // 11. Poll messages from the specific partition in topic
    let poll_messages = PollMessages {
        consumer_type: CONSUMER_TYPE,
        consumer_id: CONSUMER_ID,
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        partition_id: PARTITION_ID,
        kind: Offset,
        value: 0,
        count: MESSAGES_COUNT,
        auto_commit: false,
        format: Format::None,
    };

    let messages = client.poll_messages(&poll_messages).await.unwrap();
    assert_eq!(messages.len() as u32, MESSAGES_COUNT);
    for i in 0..MESSAGES_COUNT {
        let offset = i as u64;
        let message = messages.get(i as usize).unwrap();
        assert_message(message, offset);
    }

    // 12. Messages should be also polled in the smaller batches
    let batches_count = 10;
    let batch_size = MESSAGES_COUNT / batches_count;
    for i in 0..batches_count {
        let start_offset = (i * batch_size) as u64;
        let poll_messages = PollMessages {
            consumer_type: CONSUMER_TYPE,
            consumer_id: CONSUMER_ID,
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            partition_id: PARTITION_ID,
            kind: Offset,
            value: start_offset,
            count: batch_size,
            auto_commit: false,
            format: Format::None,
        };

        let messages = client.poll_messages(&poll_messages).await.unwrap();
        assert_eq!(messages.len() as u32, batch_size);
        for i in 0..batch_size as u64 {
            let offset = start_offset + i;
            let message = messages.get(i as usize).unwrap();
            assert_message(message, offset);
        }
    }

    // 13. Get topic details and validate the partition details
    let topic = client
        .get_topic(&GetTopic {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
        })
        .await
        .unwrap();
    assert_eq!(topic.id, TOPIC_ID);
    assert_eq!(topic.name, TOPIC_NAME);
    assert_eq!(topic.partitions_count, PARTITIONS_COUNT);
    assert_eq!(topic.partitions.len(), PARTITIONS_COUNT as usize);
    assert!(topic.size_bytes > 0);
    assert_eq!(topic.messages_count, MESSAGES_COUNT as u64);
    let topic_partition = topic.partitions.get((PARTITION_ID - 1) as usize).unwrap();
    assert_eq!(topic_partition.id, PARTITION_ID);
    assert_eq!(topic_partition.segments_count, 1);
    assert!(topic_partition.size_bytes > 0);
    assert_eq!(topic_partition.current_offset, (MESSAGES_COUNT - 1) as u64);
    assert_eq!(topic_partition.messages_count, MESSAGES_COUNT as u64);

    // 14. Ensure that messages do not exist in the second partition in the same topic
    let poll_messages = PollMessages {
        consumer_type: CONSUMER_TYPE,
        consumer_id: CONSUMER_ID,
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        partition_id: PARTITION_ID + 1,
        kind: Offset,
        value: 0,
        count: MESSAGES_COUNT,
        auto_commit: false,
        format: Format::None,
    };
    let messages = client.poll_messages(&poll_messages).await.unwrap();
    assert!(messages.is_empty());

    // 15. Get the existing customer offset and ensure it's 0
    let offset = client
        .get_offset(&GetOffset {
            consumer_type: CONSUMER_TYPE,
            consumer_id: CONSUMER_ID,
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            partition_id: PARTITION_ID,
        })
        .await
        .unwrap();
    assert_eq!(offset.consumer_id, CONSUMER_ID);
    assert_eq!(offset.offset, 0);

    // 16. Store the consumer offset
    let stored_offset = 10;
    client
        .store_offset(&StoreOffset {
            consumer_type: CONSUMER_TYPE,
            consumer_id: CONSUMER_ID,
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            partition_id: PARTITION_ID,
            offset: stored_offset,
        })
        .await
        .unwrap();

    // 17. Get the existing customer offset and ensure it's the previously stored value
    let offset = client
        .get_offset(&GetOffset {
            consumer_type: CONSUMER_TYPE,
            consumer_id: CONSUMER_ID,
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            partition_id: PARTITION_ID,
        })
        .await
        .unwrap();
    assert_eq!(offset.consumer_id, CONSUMER_ID);
    assert_eq!(offset.offset, stored_offset);

    // 18. Poll messages from the specific partition in topic using next with auto commit
    let messages_count = 10;
    let poll_messages = PollMessages {
        consumer_type: CONSUMER_TYPE,
        consumer_id: CONSUMER_ID,
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        partition_id: PARTITION_ID,
        kind: Next,
        value: 0,
        count: messages_count,
        auto_commit: true,
        format: Format::None,
    };

    let messages = client.poll_messages(&poll_messages).await.unwrap();
    assert_eq!(messages.len() as u32, messages_count);
    let first_offset = messages.first().unwrap().offset;
    let last_offset = messages.last().unwrap().offset;
    let expected_last_offset = stored_offset + messages_count as u64;
    assert_eq!(first_offset, stored_offset + 1);
    assert_eq!(last_offset, expected_last_offset);

    // 19. Get the existing customer offset and ensure that auto commit during poll has worked
    let offset = client
        .get_offset(&GetOffset {
            consumer_type: CONSUMER_TYPE,
            consumer_id: CONSUMER_ID,
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            partition_id: PARTITION_ID,
        })
        .await
        .unwrap();
    assert_eq!(offset.consumer_id, CONSUMER_ID);
    assert_eq!(offset.offset, expected_last_offset);

    // 20. Get the consumer groups and validate that there are no groups
    let consumer_groups = client
        .get_consumer_groups(&GetConsumerGroups {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
        })
        .await
        .unwrap();

    assert!(consumer_groups.is_empty());

    // 21. Create the consumer group
    client
        .create_consumer_group(&CreateConsumerGroup {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            consumer_group_id: CONSUMER_GROUP_ID,
        })
        .await
        .unwrap();

    // 22. Get the consumer groups and validate that there is one group
    let consumer_groups = client
        .get_consumer_groups(&GetConsumerGroups {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
        })
        .await
        .unwrap();

    assert_eq!(consumer_groups.len(), 1);
    let consumer_group = consumer_groups.get(0).unwrap();
    assert_eq!(consumer_group.id, CONSUMER_GROUP_ID);
    assert_eq!(consumer_group.partitions_count, PARTITIONS_COUNT);
    assert_eq!(consumer_group.members_count, 0);

    // 23. Get the consumer group details
    let consumer_group = client
        .get_consumer_group(&GetConsumerGroup {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            consumer_group_id: CONSUMER_GROUP_ID,
        })
        .await
        .unwrap();

    assert_eq!(consumer_group.id, CONSUMER_GROUP_ID);
    assert_eq!(consumer_group.partitions_count, PARTITIONS_COUNT);
    assert_eq!(consumer_group.members_count, 0);
    assert!(consumer_group.members.is_empty());

    // 24. Join the consumer group and then leave it if the feature is available
    let result = client
        .join_consumer_group(&JoinConsumerGroup {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            consumer_group_id: CONSUMER_GROUP_ID,
        })
        .await;

    match result {
        Ok(_) => {
            let consumer_group = client
                .get_consumer_group(&GetConsumerGroup {
                    stream_id: STREAM_ID,
                    topic_id: TOPIC_ID,
                    consumer_group_id: CONSUMER_GROUP_ID,
                })
                .await
                .unwrap();
            assert_eq!(consumer_group.id, CONSUMER_GROUP_ID);
            assert_eq!(consumer_group.partitions_count, PARTITIONS_COUNT);
            assert_eq!(consumer_group.members_count, 1);
            assert_eq!(consumer_group.members.len(), 1);
            let member = &consumer_group.members[0];
            assert_eq!(member.partitions_count, PARTITIONS_COUNT);

            let me = client.get_me(&GetMe {}).await.unwrap();
            assert!(me.id > 0);
            assert_eq!(me.consumer_groups_count, 1);
            assert_eq!(me.consumer_groups.len(), 1);
            let consumer_group = &me.consumer_groups[0];
            assert_eq!(consumer_group.consumer_group_id, CONSUMER_GROUP_ID);
            assert_eq!(consumer_group.topic_id, TOPIC_ID);
            assert_eq!(consumer_group.consumer_group_id, STREAM_ID);

            client
                .leave_consumer_group(&LeaveConsumerGroup {
                    stream_id: STREAM_ID,
                    topic_id: TOPIC_ID,
                    consumer_group_id: CONSUMER_GROUP_ID,
                })
                .await
                .unwrap();

            let consumer_group = client
                .get_consumer_group(&GetConsumerGroup {
                    stream_id: STREAM_ID,
                    topic_id: TOPIC_ID,
                    consumer_group_id: CONSUMER_GROUP_ID,
                })
                .await
                .unwrap();
            assert_eq!(consumer_group.members_count, 0);
            assert!(consumer_group.members.is_empty());

            let me = client.get_me(&GetMe {}).await.unwrap();
            assert_eq!(me.consumer_groups_count, 0);
            assert!(me.consumer_groups.is_empty());
        }
        Err(e) => assert_eq!(e.as_code(), Error::FeatureUnavailable.as_code()),
    }

    // 25. Get the stats and validate that there is one stream
    let stats = client.get_stats(&GetStats {}).await.unwrap();
    assert!(stats.process_id > 0);
    assert!(!stats.hostname.is_empty());
    assert!(!stats.os_name.is_empty());
    assert!(!stats.os_version.is_empty());
    assert!(!stats.kernel_version.is_empty());
    assert!(stats.memory_usage > 0);
    assert!(stats.total_memory > 0);
    assert!(stats.available_memory > 0);
    assert!(stats.run_time > 0);
    assert!(stats.start_time > 0);
    assert_eq!(stats.streams_count, 1);
    assert_eq!(stats.topics_count, 1);
    assert_eq!(stats.partitions_count, PARTITIONS_COUNT);
    assert_eq!(stats.segments_count, PARTITIONS_COUNT);
    assert_eq!(stats.messages_count, MESSAGES_COUNT as u64);

    // 26. Delete the consumer group
    client
        .delete_consumer_group(&DeleteConsumerGroup {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            consumer_group_id: CONSUMER_GROUP_ID,
        })
        .await
        .unwrap();

    // 27. Delete the existing topic and ensure it doesn't exist anymore
    client
        .delete_topic(&DeleteTopic {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
        })
        .await
        .unwrap();
    let topics = client
        .get_topics(&GetTopics {
            stream_id: STREAM_ID,
        })
        .await
        .unwrap();
    assert!(topics.is_empty());

    // 28. Delete the existing stream and ensure it doesn't exist anymore
    client
        .delete_stream(&DeleteStream {
            stream_id: STREAM_ID,
        })
        .await
        .unwrap();
    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert!(streams.is_empty());

    // 29. Get clients and ensure that there's 0 (HTTP) or 1 (TCP, QUIC) client
    let clients = client.get_clients(&GetClients {}).await.unwrap();

    assert!(clients.len() <= 1);

    test_server.stop();
}

fn assert_message(message: &iggy::models::message::Message, offset: u64) {
    let expected_payload = get_message_payload(offset);
    assert!(message.timestamp > 0);
    assert_eq!(message.offset, offset);
    assert_eq!(message.payload, expected_payload);
}

fn get_message_payload(offset: u64) -> Bytes {
    Bytes::from(format!("message {}", offset))
}
