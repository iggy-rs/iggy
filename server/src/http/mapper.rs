use sdk::models::consumer_group::{ConsumerGroupDetails, ConsumerGroupMember};
use sdk::models::stream::StreamDetails;
use sdk::models::topic::TopicDetails;
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

pub fn map_clients(clients: &[&Client]) -> Vec<sdk::models::client_info::ClientInfo> {
    let mut clients = clients
        .iter()
        .map(|client| sdk::models::client_info::ClientInfo {
            id: client.id,
            transport: client.transport.to_string(),
            address: client.address.to_string(),
        })
        .collect::<Vec<sdk::models::client_info::ClientInfo>>();
    clients.sort_by(|a, b| a.id.cmp(&b.id));
    clients
}

pub async fn map_consumer_groups(
    consumer_groups: &[&RwLock<ConsumerGroup>],
) -> Vec<sdk::models::consumer_group::ConsumerGroup> {
    let mut groups = Vec::new();
    for consumer_group in consumer_groups {
        let consumer_group = consumer_group.read().await;
        let consumer_group = sdk::models::consumer_group::ConsumerGroup {
            id: consumer_group.id,
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
