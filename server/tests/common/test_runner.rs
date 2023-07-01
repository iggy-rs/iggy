use crate::common::{ClientFactory, TestServer};
use sdk::error::Error;
use sdk::groups::create_group::CreateGroup;
use sdk::groups::delete_group::DeleteGroup;
use sdk::groups::get_group::GetGroup;
use sdk::groups::get_groups::GetGroups;
use sdk::groups::join_group::JoinGroup;
use sdk::groups::leave_group::LeaveGroup;
use sdk::messages::poll_messages::Kind::{Next, Offset};
use sdk::messages::poll_messages::{ConsumerType, Format, PollMessages};
use sdk::messages::send_messages::{KeyKind, Message, SendMessages};
use sdk::offsets::get_offset::GetOffset;
use sdk::offsets::store_offset::StoreOffset;
use sdk::streams::create_stream::CreateStream;
use sdk::streams::delete_stream::DeleteStream;
use sdk::streams::get_stream::GetStream;
use sdk::streams::get_streams::GetStreams;
use sdk::system::get_clients::GetClients;
use sdk::system::ping::Ping;
use sdk::topics::create_topic::CreateTopic;
use sdk::topics::delete_topic::DeleteTopic;
use sdk::topics::get_topic::GetTopic;
use sdk::topics::get_topics::GetTopics;
use tokio::time::sleep;

pub async fn run(client_factory: &dyn ClientFactory) {
    let test_server = TestServer::default();
    test_server.start();
    sleep(std::time::Duration::from_secs(1)).await;
    let client = client_factory.create_client().await;

    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let stream_name = "test-stream";
    let topic_name = "test-topic";
    let partitions_count = 2;
    let consumer_id = 1;
    let consumer_type = ConsumerType::Consumer;

    // 1. Ping server
    let ping = Ping {};
    client.ping(&ping).await.unwrap();

    // 2. Ensure that streams do not exist
    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert!(streams.is_empty());

    // 3. Create the stream
    let create_stream = CreateStream {
        stream_id,
        name: stream_name.to_string(),
    };
    client.create_stream(&create_stream).await.unwrap();

    // 4. Get streams and validate that created stream exists
    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert_eq!(streams.len(), 1);
    let stream = streams.get(0).unwrap();
    assert_eq!(stream.id, stream_id);
    assert_eq!(stream.name, stream_name);
    assert_eq!(stream.topics_count, 0);

    // 5. Get stream details
    let stream = client.get_stream(&GetStream { stream_id }).await.unwrap();
    assert_eq!(stream.id, stream_id);
    assert_eq!(stream.name, stream_name);
    assert_eq!(stream.topics_count, 0);
    assert!(stream.topics.is_empty());

    // 6. Create the topic
    let create_topic = CreateTopic {
        stream_id,
        topic_id,
        partitions_count,
        name: topic_name.to_string(),
    };
    client.create_topic(&create_topic).await.unwrap();

    // 7. Get topics and validate that created topic exists
    let topics = client.get_topics(&GetTopics { stream_id }).await.unwrap();
    assert_eq!(topics.len(), 1);
    let topic = topics.get(0).unwrap();
    assert_eq!(topic.id, topic_id);
    assert_eq!(topic.name, topic_name);
    assert_eq!(topic.partitions_count, partitions_count);

    // 8. Get topic details
    let topic = client
        .get_topic(&GetTopic {
            stream_id,
            topic_id,
        })
        .await
        .unwrap();
    assert_eq!(topic.id, topic_id);
    assert_eq!(topic.name, topic_name);
    assert_eq!(topic.partitions_count, partitions_count);
    assert_eq!(topic.partitions.len(), partitions_count as usize);
    let mut id = 1;
    for topic_partition in topic.partitions {
        assert_eq!(topic_partition.id, id);
        assert_eq!(topic_partition.segments_count, 1);
        assert_eq!(topic_partition.size_bytes, 0);
        assert_eq!(topic_partition.current_offset, 0);
        id += 1;
    }

    // 9. Get stream details and validate that created topic exists
    let stream = client.get_stream(&GetStream { stream_id }).await.unwrap();
    assert_eq!(stream.id, stream_id);
    assert_eq!(stream.name, stream_name);
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
        stream_id,
        topic_id,
        key_kind: KeyKind::PartitionId,
        key_value: partition_id,
        messages_count,
        messages,
    };
    client.send_messages(&send_messages).await.unwrap();

    // 11. Poll messages from the specific partition in topic
    let poll_messages = PollMessages {
        consumer_type,
        consumer_id,
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

    // 12. Messages should be also polled in the smaller batches
    let batches_count = 10;
    let batch_size = messages_count / batches_count;
    for i in 0..batches_count {
        let start_offset = (i * batch_size) as u64;
        let poll_messages = PollMessages {
            consumer_type,
            consumer_id,
            stream_id,
            topic_id,
            partition_id,
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
            stream_id,
            topic_id,
        })
        .await
        .unwrap();
    assert_eq!(topic.id, topic_id);
    assert_eq!(topic.name, topic_name);
    assert_eq!(topic.partitions_count, partitions_count);
    assert_eq!(topic.partitions.len(), partitions_count as usize);
    let topic_partition = topic.partitions.get((partition_id - 1) as usize).unwrap();
    assert_eq!(topic_partition.id, partition_id);
    assert_eq!(topic_partition.segments_count, 1);
    assert!(topic_partition.size_bytes > 0);
    assert_eq!(topic_partition.current_offset, (messages_count - 1) as u64);

    // 14. Ensure that messages do not exist in the second partition in the same topic
    let poll_messages = PollMessages {
        consumer_type,
        consumer_id,
        stream_id,
        topic_id,
        partition_id: partition_id + 1,
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
            stream_id,
            topic_id,
            partition_id,
            consumer_id,
        })
        .await
        .unwrap();
    assert_eq!(offset.consumer_id, consumer_id);
    assert_eq!(offset.offset, 0);

    // 16. Store the consumer offset
    let stored_offset = 10;
    client
        .store_offset(&StoreOffset {
            stream_id,
            topic_id,
            partition_id,
            consumer_id,
            offset: stored_offset,
        })
        .await
        .unwrap();

    // 17. Get the existing customer offset and ensure it's the previously stored value
    let offset = client
        .get_offset(&GetOffset {
            stream_id,
            topic_id,
            partition_id,
            consumer_id,
        })
        .await
        .unwrap();
    assert_eq!(offset.consumer_id, consumer_id);
    assert_eq!(offset.offset, stored_offset);

    // 18. Poll messages from the specific partition in topic using next with auto commit
    let messages_count = 10;
    let poll_messages = PollMessages {
        consumer_type,
        consumer_id,
        stream_id,
        topic_id,
        partition_id,
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
            stream_id,
            topic_id,
            partition_id,
            consumer_id,
        })
        .await
        .unwrap();
    assert_eq!(offset.consumer_id, consumer_id);
    assert_eq!(offset.offset, expected_last_offset);

    // 20. Get the consumer groups and validate that there are no groups
    let groups = client
        .get_groups(&GetGroups {
            stream_id,
            topic_id,
        })
        .await
        .unwrap();

    assert!(groups.is_empty());

    // 21. Create the consumer group
    let group_id = 1;
    client
        .create_group(&CreateGroup {
            stream_id,
            topic_id,
            group_id,
        })
        .await
        .unwrap();

    // 22. Get the consumer groups and validate that there is one group
    let groups = client
        .get_groups(&GetGroups {
            stream_id,
            topic_id,
        })
        .await
        .unwrap();

    assert_eq!(groups.len(), 1);
    let group = groups.get(0).unwrap();
    assert_eq!(group.id, group_id);
    assert_eq!(group.members_count, 0);

    // 23. Get the consumer group details
    let group = client
        .get_group(&GetGroup {
            stream_id,
            topic_id,
            group_id,
        })
        .await
        .unwrap();

    assert_eq!(group.id, group_id);
    assert_eq!(group.members_count, 0);
    assert!(group.members.is_empty());

    // 24. Join the consumer group and then leave it if the feature is available
    let result = client
        .join_group(&JoinGroup {
            stream_id,
            topic_id,
            group_id,
        })
        .await;

    match result {
        Ok(_) => {
            let group = client
                .get_group(&GetGroup {
                    stream_id,
                    topic_id,
                    group_id,
                })
                .await
                .unwrap();
            assert_eq!(group.members_count, 1);
            assert_eq!(group.members.len(), 1);
            let member = &group.members[0];
            assert_eq!(member.partitions_count, partitions_count);

            client
                .leave_group(&LeaveGroup {
                    stream_id,
                    topic_id,
                    group_id,
                })
                .await
                .unwrap();

            let group = client
                .get_group(&GetGroup {
                    stream_id,
                    topic_id,
                    group_id,
                })
                .await
                .unwrap();
            assert_eq!(group.members_count, 0);
            assert!(group.members.is_empty())
        }
        Err(e) => assert_eq!(e.as_code(), Error::FeatureUnavailable.as_code()),
    }

    // 25. Delete the consumer group
    client
        .delete_group(&DeleteGroup {
            stream_id,
            topic_id,
            group_id,
        })
        .await
        .unwrap();

    // 26. Delete the existing topic and ensure it doesn't exist anymore
    client
        .delete_topic(&DeleteTopic {
            stream_id,
            topic_id,
        })
        .await
        .unwrap();
    let topics = client.get_topics(&GetTopics { stream_id }).await.unwrap();
    assert!(topics.is_empty());

    // 27. Delete the existing stream and ensure it doesn't exist anymore
    client
        .delete_stream(&DeleteStream { stream_id })
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

fn get_message_payload(offset: u64) -> Vec<u8> {
    format!("message {}", offset).as_bytes().to_vec()
}
