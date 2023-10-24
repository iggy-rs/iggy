use bytes::Bytes;
use iggy::client::{
    ConsumerGroupClient, ConsumerOffsetClient, MessageClient, PartitionClient, StreamClient,
    SystemClient, TopicClient, UserClient,
};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::consumer::{Consumer, ConsumerKind};
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::get_consumer_groups::GetConsumerGroups;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::error::Error;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingStrategy};
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::streams::get_stream::GetStream;
use iggy::streams::get_streams::GetStreams;
use iggy::streams::update_stream::UpdateStream;
use iggy::system::get_clients::GetClients;
use iggy::system::get_me::GetMe;
use iggy::system::get_stats::GetStats;
use iggy::system::ping::Ping;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::topics::get_topic::GetTopic;
use iggy::topics::get_topics::GetTopics;
use iggy::topics::update_topic::UpdateTopic;
use iggy::users::defaults::*;
use iggy::users::login_user::LoginUser;
use integration::test_server::{assert_clean_system, ClientFactory};

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;
const PARTITIONS_COUNT: u32 = 3;
const CONSUMER_ID: u32 = 1;
const CONSUMER_KIND: ConsumerKind = ConsumerKind::Consumer;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const CONSUMER_GROUP_ID: u32 = 10;
const CONSUMER_GROUP_NAME: &str = "test-consumer-group";
const MESSAGES_COUNT: u32 = 1000;

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, IggyClientConfig::default(), None, None, None);

    // 0. Ping server
    let ping = Ping {};
    client.ping(&ping).await.unwrap();

    // 1. Login as root user
    client
        .login_user(&LoginUser {
            username: DEFAULT_ROOT_USERNAME.to_string(),
            password: DEFAULT_ROOT_PASSWORD.to_string(),
        })
        .await
        .unwrap();

    // 2. Ensure that streams do not exist
    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert!(streams.is_empty());

    // 3. Create the stream
    let mut create_stream = CreateStream {
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

    // 5. Get stream details by ID
    let stream = client
        .get_stream(&GetStream {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(stream.id, STREAM_ID);
    assert_eq!(stream.name, STREAM_NAME);
    assert_eq!(stream.topics_count, 0);
    assert!(stream.topics.is_empty());
    assert_eq!(stream.size_bytes, 0);
    assert_eq!(stream.messages_count, 0);

    // 6. Get stream details by name
    let stream = client
        .get_stream(&GetStream {
            stream_id: Identifier::named(STREAM_NAME).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(stream.id, STREAM_ID);
    assert_eq!(stream.name, STREAM_NAME);

    // 7. Try to create the stream with the same ID but the different name and validate that it fails
    create_stream.name = format!("{}-2", STREAM_NAME);
    let create_stream_result = client.create_stream(&create_stream).await;
    assert!(create_stream_result.is_err());

    // 8. Try to create the stream with the same name but the different ID and validate that it fails
    create_stream.stream_id = STREAM_ID + 1;
    create_stream.name = STREAM_NAME.to_string();
    let create_stream_result = client.create_stream(&create_stream).await;
    assert!(create_stream_result.is_err());

    // 9. Create the topic
    let mut create_topic = CreateTopic {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: TOPIC_ID,
        partitions_count: PARTITIONS_COUNT,
        name: TOPIC_NAME.to_string(),
        message_expiry: None,
    };
    client.create_topic(&create_topic).await.unwrap();

    // 10. Get topics and validate that created topic exists
    let topics = client
        .get_topics(&GetTopics {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
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
    assert_eq!(topic.message_expiry, None);

    // 11. Get topic details by ID
    let topic = client
        .get_topic(&GetTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
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

    // 12. Get topic details by name
    let topic = client
        .get_topic(&GetTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::named(TOPIC_NAME).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(topic.id, TOPIC_ID);
    assert_eq!(topic.name, TOPIC_NAME);

    // 13. Get stream details and validate that created topic exists
    let stream = client
        .get_stream(&GetStream {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
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

    // 15. Try to create the topic with the same ID but the different name and validate that it fails
    create_topic.name = format!("{}-2", TOPIC_NAME);
    let create_topic_result = client.create_topic(&create_topic).await;
    assert!(create_topic_result.is_err());

    // 16. Try to create the topic with the different ID but the same name and validate that it fails
    create_topic.topic_id = TOPIC_ID + 1;
    create_topic.name = TOPIC_NAME.to_string();
    let create_topic_result = client.create_topic(&create_topic).await;
    assert!(create_topic_result.is_err());

    // 17. Send messages to the specific topic and partition
    let mut messages = Vec::new();
    for offset in 0..MESSAGES_COUNT {
        let id = (offset + 1) as u128;
        let payload = get_message_payload(offset as u64);
        messages.push(Message {
            id,
            length: payload.len() as u32,
            payload,
            headers: None,
        });
    }

    let mut send_messages = SendMessages {
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        partitioning: Partitioning::partition_id(PARTITION_ID),
        messages,
    };
    client.send_messages(&mut send_messages).await.unwrap();

    // 18. Poll messages from the specific partition in topic
    let poll_messages = PollMessages {
        consumer: Consumer {
            kind: CONSUMER_KIND,
            id: Identifier::numeric(CONSUMER_ID).unwrap(),
        },
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        partition_id: Some(PARTITION_ID),
        strategy: PollingStrategy::offset(0),
        count: MESSAGES_COUNT,
        auto_commit: false,
    };

    let polled_messages = client.poll_messages(&poll_messages).await.unwrap();
    assert_eq!(polled_messages.messages.len() as u32, MESSAGES_COUNT);
    for i in 0..MESSAGES_COUNT {
        let offset = i as u64;
        let message = polled_messages.messages.get(i as usize).unwrap();
        assert_message(message, offset);
    }

    // 19. Messages should be also polled in the smaller batches
    let batches_count = 10;
    let batch_size = MESSAGES_COUNT / batches_count;
    for i in 0..batches_count {
        let start_offset = (i * batch_size) as u64;
        let poll_messages = PollMessages {
            consumer: Consumer {
                kind: CONSUMER_KIND,
                id: Identifier::numeric(CONSUMER_ID).unwrap(),
            },
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            partition_id: Some(PARTITION_ID),
            strategy: PollingStrategy::offset(start_offset),
            count: batch_size,
            auto_commit: false,
        };

        let polled_messages = client.poll_messages(&poll_messages).await.unwrap();
        assert_eq!(polled_messages.messages.len() as u32, batch_size);
        for i in 0..batch_size as u64 {
            let offset = start_offset + i;
            let message = polled_messages.messages.get(i as usize).unwrap();
            assert_message(message, offset);
        }
    }

    // 20. Get topic details and validate the partition details
    let topic = client
        .get_topic(&GetTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
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

    // 21. Ensure that messages do not exist in the second partition in the same topic
    let poll_messages = PollMessages {
        consumer: Consumer {
            kind: CONSUMER_KIND,
            id: Identifier::numeric(CONSUMER_ID).unwrap(),
        },
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        partition_id: Some(PARTITION_ID + 1),
        strategy: PollingStrategy::offset(0),
        count: MESSAGES_COUNT,
        auto_commit: false,
    };
    let polled_messages = client.poll_messages(&poll_messages).await.unwrap();
    assert!(polled_messages.messages.is_empty());

    // 22. Get the existing customer offset and ensure it's 0
    let offset = client
        .get_consumer_offset(&GetConsumerOffset {
            consumer: Consumer {
                kind: CONSUMER_KIND,
                id: Identifier::numeric(CONSUMER_ID).unwrap(),
            },
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            partition_id: Some(PARTITION_ID),
        })
        .await
        .unwrap();
    assert_eq!(offset.partition_id, PARTITION_ID);
    assert_eq!(offset.current_offset, (MESSAGES_COUNT - 1) as u64);
    assert_eq!(offset.stored_offset, 0);

    // 23. Store the consumer offset
    let stored_offset = 10;
    client
        .store_consumer_offset(&StoreConsumerOffset {
            consumer: Consumer {
                kind: CONSUMER_KIND,
                id: Identifier::numeric(CONSUMER_ID).unwrap(),
            },
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            partition_id: Some(PARTITION_ID),
            offset: stored_offset,
        })
        .await
        .unwrap();

    // 24. Get the existing customer offset and ensure it's the previously stored value
    let offset = client
        .get_consumer_offset(&GetConsumerOffset {
            consumer: Consumer {
                kind: CONSUMER_KIND,
                id: Identifier::numeric(CONSUMER_ID).unwrap(),
            },
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            partition_id: Some(PARTITION_ID),
        })
        .await
        .unwrap();
    assert_eq!(offset.partition_id, PARTITION_ID);
    assert_eq!(offset.current_offset, (MESSAGES_COUNT - 1) as u64);
    assert_eq!(offset.stored_offset, stored_offset);

    // 25. Poll messages from the specific partition in topic using next with auto commit
    let messages_count = 10;
    let poll_messages = PollMessages {
        consumer: Consumer {
            kind: CONSUMER_KIND,
            id: Identifier::numeric(CONSUMER_ID).unwrap(),
        },
        stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        partition_id: Some(PARTITION_ID),
        strategy: PollingStrategy::next(),
        count: messages_count,
        auto_commit: true,
    };

    let polled_messages = client.poll_messages(&poll_messages).await.unwrap();
    assert_eq!(polled_messages.messages.len() as u32, messages_count);
    let first_offset = polled_messages.messages.first().unwrap().offset;
    let last_offset = polled_messages.messages.last().unwrap().offset;
    let expected_last_offset = stored_offset + messages_count as u64;
    assert_eq!(first_offset, stored_offset + 1);
    assert_eq!(last_offset, expected_last_offset);

    // 26. Get the existing customer offset and ensure that auto commit during poll has worked
    let offset = client
        .get_consumer_offset(&GetConsumerOffset {
            consumer: Consumer {
                kind: CONSUMER_KIND,
                id: Identifier::numeric(CONSUMER_ID).unwrap(),
            },
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            partition_id: Some(PARTITION_ID),
        })
        .await
        .unwrap();
    assert_eq!(offset.partition_id, PARTITION_ID);
    assert_eq!(offset.current_offset, (MESSAGES_COUNT - 1) as u64);
    assert_eq!(offset.stored_offset, expected_last_offset);

    // 27. Get the consumer groups and validate that there are no groups
    let consumer_groups = client
        .get_consumer_groups(&GetConsumerGroups {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        })
        .await
        .unwrap();

    assert!(consumer_groups.is_empty());

    // 28. Create the consumer group
    client
        .create_consumer_group(&CreateConsumerGroup {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            consumer_group_id: CONSUMER_GROUP_ID,
            name: CONSUMER_GROUP_NAME.to_string(),
        })
        .await
        .unwrap();

    // 29. Get the consumer groups and validate that there is one group
    let consumer_groups = client
        .get_consumer_groups(&GetConsumerGroups {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        })
        .await
        .unwrap();

    assert_eq!(consumer_groups.len(), 1);
    let consumer_group = consumer_groups.get(0).unwrap();
    assert_eq!(consumer_group.id, CONSUMER_GROUP_ID);
    assert_eq!(consumer_group.partitions_count, PARTITIONS_COUNT);
    assert_eq!(consumer_group.members_count, 0);

    // 30. Get the consumer group details
    let consumer_group = client
        .get_consumer_group(&GetConsumerGroup {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            consumer_group_id: Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
        })
        .await
        .unwrap();

    assert_eq!(consumer_group.id, CONSUMER_GROUP_ID);
    assert_eq!(consumer_group.partitions_count, PARTITIONS_COUNT);
    assert_eq!(consumer_group.members_count, 0);
    assert!(consumer_group.members.is_empty());

    // 31. Join the consumer group and then leave it if the feature is available
    let result = client
        .join_consumer_group(&JoinConsumerGroup {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            consumer_group_id: Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
        })
        .await;

    match result {
        Ok(_) => {
            let consumer_group = client
                .get_consumer_group(&GetConsumerGroup {
                    stream_id: Identifier::numeric(STREAM_ID).unwrap(),
                    topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
                    consumer_group_id: Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
                })
                .await
                .unwrap();
            assert_eq!(consumer_group.id, CONSUMER_GROUP_ID);
            assert_eq!(consumer_group.partitions_count, PARTITIONS_COUNT);
            assert_eq!(consumer_group.name, CONSUMER_GROUP_NAME);
            assert_eq!(consumer_group.members_count, 1);
            assert_eq!(consumer_group.members.len(), 1);
            let member = &consumer_group.members[0];
            assert_eq!(member.partitions_count, PARTITIONS_COUNT);

            let me = client.get_me(&GetMe {}).await.unwrap();
            assert!(me.client_id > 0);
            assert_eq!(me.consumer_groups_count, 1);
            assert_eq!(me.consumer_groups.len(), 1);
            let consumer_group = &me.consumer_groups[0];
            assert_eq!(consumer_group.stream_id, STREAM_ID);
            assert_eq!(consumer_group.topic_id, TOPIC_ID);
            assert_eq!(consumer_group.consumer_group_id, CONSUMER_GROUP_ID);

            client
                .leave_consumer_group(&LeaveConsumerGroup {
                    stream_id: Identifier::numeric(STREAM_ID).unwrap(),
                    topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
                    consumer_group_id: Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
                })
                .await
                .unwrap();

            let consumer_group = client
                .get_consumer_group(&GetConsumerGroup {
                    stream_id: Identifier::numeric(STREAM_ID).unwrap(),
                    topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
                    consumer_group_id: Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
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

    // 32. Get the stats and validate that there is one stream
    let stats = client.get_stats(&GetStats {}).await.unwrap();
    assert!(!stats.hostname.is_empty());
    assert!(!stats.os_name.is_empty());
    assert!(!stats.os_version.is_empty());
    assert!(!stats.kernel_version.is_empty());
    assert_eq!(stats.streams_count, 1);
    assert_eq!(stats.topics_count, 1);
    assert_eq!(stats.partitions_count, PARTITIONS_COUNT);
    assert_eq!(stats.segments_count, PARTITIONS_COUNT);
    assert_eq!(stats.messages_count, MESSAGES_COUNT as u64);

    // 33. Delete the consumer group
    client
        .delete_consumer_group(&DeleteConsumerGroup {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            consumer_group_id: Identifier::numeric(CONSUMER_GROUP_ID).unwrap(),
        })
        .await
        .unwrap();

    // 34. Create new partitions and validate that the number of partitions is increased
    client
        .create_partitions(&CreatePartitions {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            partitions_count: PARTITIONS_COUNT,
        })
        .await
        .unwrap();

    let topic = client
        .get_topic(&GetTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        })
        .await
        .unwrap();

    assert_eq!(topic.partitions_count, 2 * PARTITIONS_COUNT);

    // 35. Delete the partitions and validate that the number of partitions is decreased
    client
        .delete_partitions(&DeletePartitions {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            partitions_count: PARTITIONS_COUNT,
        })
        .await
        .unwrap();

    let topic = client
        .get_topic(&GetTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        })
        .await
        .unwrap();

    assert_eq!(topic.partitions_count, PARTITIONS_COUNT);

    // 36. Update the existing topic and ensure it's updated
    let updated_topic_name = format!("{}-updated", TOPIC_NAME);
    let updated_message_expiry = 1000;

    client
        .update_topic(&UpdateTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
            name: updated_topic_name.clone(),
            message_expiry: Some(updated_message_expiry),
        })
        .await
        .unwrap();

    let updated_topic = client
        .get_topic(&GetTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        })
        .await
        .unwrap();

    assert_eq!(updated_topic.name, updated_topic_name);
    assert_eq!(updated_topic.message_expiry, Some(updated_message_expiry));

    // 37. Delete the existing topic and ensure it doesn't exist anymore
    client
        .delete_topic(&DeleteTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: Identifier::numeric(TOPIC_ID).unwrap(),
        })
        .await
        .unwrap();
    let topics = client
        .get_topics(&GetTopics {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        })
        .await
        .unwrap();
    assert!(topics.is_empty());

    // 38. Update the existing stream and ensure it's updated
    let updated_stream_name = format!("{}-updated", STREAM_NAME);

    client
        .update_stream(&UpdateStream {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            name: updated_stream_name.clone(),
        })
        .await
        .unwrap();

    let updated_stream = client
        .get_stream(&GetStream {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        })
        .await
        .unwrap();

    assert_eq!(updated_stream.name, updated_stream_name);

    // 39. Delete the existing stream and ensure it doesn't exist anymore
    client
        .delete_stream(&DeleteStream {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
        })
        .await
        .unwrap();
    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert!(streams.is_empty());

    // 40. Get clients and ensure that there's 0 (HTTP) or 1 (TCP, QUIC) client
    let clients = client.get_clients(&GetClients {}).await.unwrap();

    assert!(clients.len() <= 1);

    assert_clean_system(&client).await;
}

fn assert_message(message: &iggy::models::messages::Message, offset: u64) {
    let expected_payload = get_message_payload(offset);
    assert!(message.timestamp > 0);
    assert_eq!(message.offset, offset);
    assert_eq!(message.payload, expected_payload);
}

fn get_message_payload(offset: u64) -> Bytes {
    Bytes::from(format!("message {}", offset))
}
