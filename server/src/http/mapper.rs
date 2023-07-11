use sdk::models::client_info::ConsumerGroupInfo;
use sdk::models::consumer_group::{ConsumerGroupDetails, ConsumerGroupMember};
use sdk::models::stream::StreamDetails;
use sdk::models::topic::TopicDetails;
use std::sync::Arc;
use streaming::clients::client_manager::Client;
use streaming::streams::stream::Stream;
use streaming::topics::consumer_group::ConsumerGroup;
use streaming::topics::topic::Topic;
use tokio::sync::RwLock;

pub fn map_stream(stream: &Stream) -> StreamDetails {
    let mut stream_details = StreamDetails {
        id: stream.id,
        name: stream.name.clone(),
        topics_count: stream.get_topics().len() as u32,
        topics: stream
            .get_topics()
            .iter()
            .map(|topic| sdk::models::topic::Topic {
                id: topic.id,
                name: topic.name.clone(),
                partitions_count: topic.get_partitions().len() as u32,
            })
            .collect(),
    };
    stream_details.topics.sort_by(|a, b| a.id.cmp(&b.id));
    stream_details
}

pub fn map_streams(streams: &[&Stream]) -> Vec<sdk::models::stream::Stream> {
    let mut streams = streams
        .iter()
        .map(|stream| sdk::models::stream::Stream {
            id: stream.id,
            name: stream.name.clone(),
            topics_count: stream.get_topics().len() as u32,
        })
        .collect::<Vec<sdk::models::stream::Stream>>();
    streams.sort_by(|a, b| a.id.cmp(&b.id));
    streams
}

pub fn map_topics(topics: &[&Topic]) -> Vec<sdk::models::topic::Topic> {
    let mut topics = topics
        .iter()
        .map(|topic| sdk::models::topic::Topic {
            id: topic.id,
            name: topic.name.clone(),
            partitions_count: topic.get_partitions().len() as u32,
        })
        .collect::<Vec<sdk::models::topic::Topic>>();
    topics.sort_by(|a, b| a.id.cmp(&b.id));
    topics
}

pub async fn map_topic(topic: &Topic) -> TopicDetails {
    let mut topic_details = TopicDetails {
        id: topic.id,
        name: topic.name.clone(),
        partitions_count: topic.get_partitions().len() as u32,
        partitions: Vec::new(),
    };
    for partition in topic.get_partitions() {
        let partition = partition.read().await;
        topic_details
            .partitions
            .push(sdk::models::partition::Partition {
                id: partition.id,
                segments_count: partition.get_segments().len() as u32,
                current_offset: partition.current_offset,
                size_bytes: partition
                    .get_segments()
                    .iter()
                    .map(|segment| segment.current_size_bytes as u64)
                    .sum(),
            });
    }
    topic_details.partitions.sort_by(|a, b| a.id.cmp(&b.id));
    topic_details
}

pub async fn map_client(client: &Client) -> sdk::models::client_info::ClientInfoDetails {
    let client = sdk::models::client_info::ClientInfoDetails {
        id: client.id,
        transport: client.transport.to_string(),
        address: client.address.to_string(),
        consumer_groups_count: client.consumer_groups.len() as u32,
        consumer_groups: client
            .consumer_groups
            .iter()
            .map(|consumer_group| ConsumerGroupInfo {
                stream_id: consumer_group.stream_id,
                topic_id: consumer_group.topic_id,
                consumer_group_id: consumer_group.consumer_group_id,
            })
            .collect(),
    };
    client
}

pub async fn map_clients(
    clients: &[Arc<RwLock<Client>>],
) -> Vec<sdk::models::client_info::ClientInfo> {
    let mut all_clients = Vec::new();
    for client in clients {
        let client = client.read().await;
        let client = sdk::models::client_info::ClientInfo {
            id: client.id,
            transport: client.transport.to_string(),
            address: client.address.to_string(),
            consumer_groups_count: client.consumer_groups.len() as u32,
        };
        all_clients.push(client);
    }

    all_clients.sort_by(|a, b| a.id.cmp(&b.id));
    all_clients
}

pub async fn map_consumer_groups(
    consumer_groups: &[&RwLock<ConsumerGroup>],
) -> Vec<sdk::models::consumer_group::ConsumerGroup> {
    let mut groups = Vec::new();
    for consumer_group in consumer_groups {
        let consumer_group = consumer_group.read().await;
        let consumer_group = sdk::models::consumer_group::ConsumerGroup {
            id: consumer_group.id,
            partitions_count: consumer_group.partitions_count,
            members_count: consumer_group.get_members().len() as u32,
        };
        groups.push(consumer_group);
    }
    groups.sort_by(|a, b| a.id.cmp(&b.id));
    groups
}

pub async fn map_consumer_group(consumer_group: &ConsumerGroup) -> ConsumerGroupDetails {
    let mut consumer_group_details = ConsumerGroupDetails {
        id: consumer_group.id,
        partitions_count: consumer_group.partitions_count,
        members_count: consumer_group.get_members().len() as u32,
        members: Vec::new(),
    };
    let members = consumer_group.get_members();
    for member in members {
        let member = member.read().await;
        let partitions = member.get_partitions();
        consumer_group_details.members.push(ConsumerGroupMember {
            id: member.id,
            partitions_count: partitions.len() as u32,
            partitions,
        });
    }
    consumer_group_details
}
