use iggy::models::stats::Stats;
use std::sync::Arc;
use streaming::clients::client_manager::{Client, Transport};
use streaming::message::Message;
use streaming::partitions::partition::Partition;
use streaming::streams::stream::Stream;
use streaming::topics::consumer_group::ConsumerGroup;
use streaming::topics::topic::Topic;
use tokio::sync::RwLock;

pub fn map_stats(stats: &Stats) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(104);
    bytes.extend(stats.process_id.to_le_bytes());
    bytes.extend(stats.cpu_usage.to_le_bytes());
    bytes.extend(stats.memory_usage.to_le_bytes());
    bytes.extend(stats.total_memory.to_le_bytes());
    bytes.extend(stats.available_memory.to_le_bytes());
    bytes.extend(stats.run_time.to_le_bytes());
    bytes.extend(stats.start_time.to_le_bytes());
    bytes.extend(stats.read_bytes.to_le_bytes());
    bytes.extend(stats.written_bytes.to_le_bytes());
    bytes.extend(stats.messages_size_bytes.to_le_bytes());
    bytes.extend(stats.streams_count.to_le_bytes());
    bytes.extend(stats.topics_count.to_le_bytes());
    bytes.extend(stats.partitions_count.to_le_bytes());
    bytes.extend(stats.segments_count.to_le_bytes());
    bytes.extend(stats.messages_count.to_le_bytes());
    bytes.extend(stats.clients_count.to_le_bytes());
    bytes.extend(stats.consumer_groups_count.to_le_bytes());
    bytes.extend((stats.hostname.len() as u32).to_le_bytes());
    bytes.extend(stats.hostname.as_bytes());
    bytes.extend((stats.os_name.len() as u32).to_le_bytes());
    bytes.extend(stats.os_name.as_bytes());
    bytes.extend((stats.os_version.len() as u32).to_le_bytes());
    bytes.extend(stats.os_version.as_bytes());
    bytes.extend((stats.kernel_version.len() as u32).to_le_bytes());
    bytes.extend(stats.kernel_version.as_bytes());
    bytes
}

pub fn map_offset(consumer_id: u32, offset: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(12);
    bytes.extend(consumer_id.to_le_bytes());
    bytes.extend(offset.to_le_bytes());
    bytes
}

pub async fn map_client(client: &Client) -> Vec<u8> {
    let mut bytes = Vec::new();
    extend_client(client, &mut bytes);
    for consumer_group in &client.consumer_groups {
        bytes.extend(consumer_group.consumer_group_id.to_le_bytes());
        bytes.extend(consumer_group.topic_id.to_le_bytes());
        bytes.extend(consumer_group.stream_id.to_le_bytes());
    }
    bytes
}

pub async fn map_clients(clients: &[Arc<RwLock<Client>>]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for client in clients {
        let client = client.read().await;
        extend_client(&client, &mut bytes);
    }
    bytes
}

pub fn map_messages(messages: &[Arc<Message>]) -> Vec<u8> {
    let messages_count = messages.len() as u32;
    let messages_size = messages
        .iter()
        .map(|message| message.get_size_bytes(false))
        .sum::<u32>();

    let mut bytes = Vec::with_capacity(4 + messages_size as usize);
    bytes.extend(messages_count.to_le_bytes());
    for message in messages {
        message.extend(&mut bytes, false);
    }

    bytes
}

pub fn map_stream(stream: &Stream) -> Vec<u8> {
    let mut bytes = Vec::new();
    extend_stream(stream, &mut bytes);
    for topic in stream.get_topics() {
        extend_topic(topic, &mut bytes);
    }
    bytes
}

pub fn map_streams(streams: &[&Stream]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for stream in streams {
        extend_stream(stream, &mut bytes);
    }
    bytes
}

pub fn map_topics(topics: &[&Topic]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for topic in topics {
        extend_topic(topic, &mut bytes);
    }
    bytes
}

pub async fn map_topic(topic: &Topic) -> Vec<u8> {
    let mut bytes = Vec::new();
    extend_topic(topic, &mut bytes);
    for partition in topic.get_partitions() {
        let partition = partition.read().await;
        extend_partition(&partition, &mut bytes);
    }
    bytes
}

pub async fn map_consumer_group(consumer_group: &ConsumerGroup) -> Vec<u8> {
    let mut bytes = Vec::new();
    extend_consumer_group(consumer_group, &mut bytes);
    let members = consumer_group.get_members();
    for member in members {
        let member = member.read().await;
        bytes.extend(member.id.to_le_bytes());
        let partitions = member.get_partitions();
        bytes.extend((partitions.len() as u32).to_le_bytes());
        for partition in partitions {
            bytes.extend(partition.to_le_bytes());
        }
    }
    bytes
}

pub async fn map_consumer_groups(consumer_groups: &[&RwLock<ConsumerGroup>]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for consumer_group in consumer_groups {
        let consumer_group = consumer_group.read().await;
        extend_consumer_group(&consumer_group, &mut bytes);
    }
    bytes
}

fn extend_stream(stream: &Stream, bytes: &mut Vec<u8>) {
    bytes.extend(stream.id.to_le_bytes());
    bytes.extend((stream.get_topics().len() as u32).to_le_bytes());
    bytes.extend((stream.name.len() as u32).to_le_bytes());
    bytes.extend(stream.name.as_bytes());
}

fn extend_topic(topic: &Topic, bytes: &mut Vec<u8>) {
    bytes.extend(topic.id.to_le_bytes());
    bytes.extend((topic.get_partitions().len() as u32).to_le_bytes());
    bytes.extend((topic.name.len() as u32).to_le_bytes());
    bytes.extend(topic.name.as_bytes());
}

fn extend_partition(partition: &Partition, bytes: &mut Vec<u8>) {
    bytes.extend(partition.id.to_le_bytes());
    bytes.extend((partition.get_segments().len() as u32).to_le_bytes());
    bytes.extend(partition.current_offset.to_le_bytes());
    bytes.extend(
        partition
            .get_segments()
            .iter()
            .map(|segment| segment.current_size_bytes as u64)
            .sum::<u64>()
            .to_le_bytes(),
    );
}

fn extend_consumer_group(consumer_group: &ConsumerGroup, bytes: &mut Vec<u8>) {
    bytes.extend(consumer_group.id.to_le_bytes());
    bytes.extend(consumer_group.partitions_count.to_le_bytes());
    bytes.extend((consumer_group.get_members().len() as u32).to_le_bytes());
}

fn extend_client(client: &Client, bytes: &mut Vec<u8>) {
    bytes.extend(client.id.to_le_bytes());
    let transport: u8 = match client.transport {
        Transport::Tcp => 1,
        Transport::Quic => 2,
    };
    bytes.extend(transport.to_le_bytes());
    let address = client.address.to_string();
    bytes.extend((address.len() as u32).to_le_bytes());
    bytes.extend(address.as_bytes());
    bytes.extend((client.consumer_groups.len() as u32).to_le_bytes());
}
