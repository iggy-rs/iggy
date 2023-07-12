use crate::common::{ClientFactory, TestServer};
use bytes::Bytes;
use sdk::client::{ConsumerGroupClient, MessageClient, StreamClient, SystemClient, TopicClient};
use sdk::clients::client::{IggyClient, IggyClientConfig};
use sdk::consumer_groups::create_consumer_group::CreateConsumerGroup;
use sdk::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use sdk::consumer_groups::get_consumer_group::GetConsumerGroup;
use sdk::consumer_groups::get_consumer_groups::GetConsumerGroups;
use sdk::consumer_groups::join_consumer_group::JoinConsumerGroup;
use sdk::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use sdk::consumer_type::ConsumerType;
use sdk::error::Error;
use sdk::messages::poll_messages::Kind::{Next, Offset};
use sdk::messages::poll_messages::{Format, PollMessages};
use sdk::messages::send_messages::{KeyKind, Message, SendMessages};
use sdk::offsets::get_offset::GetOffset;
use sdk::offsets::store_offset::StoreOffset;
use sdk::streams::create_stream::CreateStream;
use sdk::streams::delete_stream::DeleteStream;
use sdk::streams::get_stream::GetStream;
use sdk::streams::get_streams::GetStreams;
use sdk::system::get_clients::GetClients;
use sdk::system::get_me::GetMe;
use sdk::system::ping::Ping;
use sdk::topics::create_topic::CreateTopic;
use sdk::topics::delete_topic::DeleteTopic;
use sdk::topics::get_topic::GetTopic;
use sdk::topics::get_topics::GetTopics;
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
    let mut id = 1;
    for topic_partition in topic.partitions {
        assert_eq!(topic_partition.id, id);
        assert_eq!(topic_partition.segments_count, 1);
        assert_eq!(topic_partition.size_bytes, 0);
        assert_eq!(topic_partition.current_offset, 0);
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
    let stream_topic = stream.topics.get(0).unwrap();
    assert_eq!(stream_topic.id, topic.id);
    assert_eq!(stream_topic.name, topic.name);
    assert_eq!(stream_topic.partitions_count, topic.partitions_count);

    // 10. Send messages to the specific topic and partition
    let messages_count = 1000u32;
    let mut messages = Vec::new();
    for offset in 0..messages_count {
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
        key_kind: KeyKind::PartitionId,
        key_value: PARTITION_ID,
        messages_count,
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

    // 12. Messages should be also polled in the smaller batches
    let batches_count = 10;
    let batch_size = messages_count / batches_count;
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
    let topic_partition = topic.partitions.get((PARTITION_ID - 1) as usize).unwrap();
    assert_eq!(topic_partition.id, PARTITION_ID);
    assert_eq!(topic_partition.segments_count, 1);
    assert!(topic_partition.size_bytes > 0);
    assert_eq!(topic_partition.current_offset, (messages_count - 1) as u64);

    // 14. Ensure that messages do not exist in the second partition in the same topic
    let poll_messages = PollMessages {
        consumer_type: CONSUMER_TYPE,
        consumer_id: CONSUMER_ID,
        stream_id: STREAM_ID,
        topic_id: TOPIC_ID,
        partition_id: PARTITION_ID + 1,
        kind: Offset,
        value: 0,
        count: messages_count,
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

    // 25. Delete the consumer group
    client
        .delete_consumer_group(&DeleteConsumerGroup {
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            consumer_group_id: CONSUMER_GROUP_ID,
        })
        .await
        .unwrap();

    // 26. Delete the existing topic and ensure it doesn't exist anymore
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

    // 27. Delete the existing stream and ensure it doesn't exist anymore
    client
        .delete_stream(&DeleteStream {
            stream_id: STREAM_ID,
        })
        .await
        .unwrap();
    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert!(streams.is_empty());

    // 28. Get clients and ensure that there's 0 (HTTP) or 1 (TCP, QUIC) client
    let clients = client.get_clients(&GetClients {}).await.unwrap();

    assert!(clients.len() <= 1);

    test_server.stop();
}

fn assert_message(message: &sdk::models::message::Message, offset: u64) {
    let expected_payload = get_message_payload(offset);
    assert!(message.timestamp > 0);
    assert_eq!(message.offset, offset);
    assert_eq!(message.payload, expected_payload);
}

fn get_message_payload(offset: u64) -> Bytes {
    Bytes::from(format!("message {}", offset))
}
