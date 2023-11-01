use crate::streaming::clients::client_manager::{Client, Transport};
use crate::streaming::models::messages::PolledMessages;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use crate::streaming::users::user::User;
use bytes::BufMut;
use iggy::bytes_serializable::BytesSerializable;
use iggy::models::consumer_offset_info::ConsumerOffsetInfo;
use iggy::models::stats::Stats;
use iggy::models::user_info::UserId;
use std::sync::Arc;
use tokio::sync::RwLock;

pub fn map_stats(stats: &Stats) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(104);
    bytes.put_u32_le(stats.process_id);
    bytes.put_f32_le(stats.cpu_usage);
    bytes.put_u64_le(stats.memory_usage);
    bytes.put_u64_le(stats.total_memory);
    bytes.put_u64_le(stats.available_memory);
    bytes.put_u64_le(stats.run_time);
    bytes.put_u64_le(stats.start_time);
    bytes.put_u64_le(stats.read_bytes);
    bytes.put_u64_le(stats.written_bytes);
    bytes.put_u64_le(stats.messages_size_bytes);
    bytes.put_u32_le(stats.streams_count);
    bytes.put_u32_le(stats.topics_count);
    bytes.put_u32_le(stats.partitions_count);
    bytes.put_u32_le(stats.segments_count);
    bytes.put_u64_le(stats.messages_count);
    bytes.put_u32_le(stats.clients_count);
    bytes.put_u32_le(stats.consumer_groups_count);
    bytes.put_u32_le(stats.hostname.len() as u32);
    bytes.extend(stats.hostname.as_bytes());
    bytes.put_u32_le(stats.os_name.len() as u32);
    bytes.extend(stats.os_name.as_bytes());
    bytes.put_u32_le(stats.os_version.len() as u32);
    bytes.extend(stats.os_version.as_bytes());
    bytes.put_u32_le(stats.kernel_version.len() as u32);
    bytes.extend(stats.kernel_version.as_bytes());
    bytes
}

pub fn map_consumer_offset(offset: &ConsumerOffsetInfo) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20);
    bytes.put_u32_le(offset.partition_id);
    bytes.put_u64_le(offset.current_offset);
    bytes.put_u64_le(offset.stored_offset);
    bytes
}

pub async fn map_client(client: &Client) -> Vec<u8> {
    let mut bytes = Vec::new();
    extend_client(client, &mut bytes);
    for consumer_group in &client.consumer_groups {
        bytes.put_u32_le(consumer_group.stream_id);
        bytes.put_u32_le(consumer_group.topic_id);
        bytes.put_u32_le(consumer_group.consumer_group_id);
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

pub fn map_user(user: &User) -> Vec<u8> {
    let mut bytes = Vec::new();
    extend_user(user, &mut bytes);
    if let Some(permissions) = &user.permissions {
        bytes.put_u8(1);
        let permissions = permissions.as_bytes();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u32_le(permissions.len() as u32);
        bytes.extend(permissions);
    } else {
        bytes.put_u32_le(0);
    }
    bytes
}

pub fn map_users(users: &[User]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for user in users {
        extend_user(user, &mut bytes);
    }
    bytes
}

pub fn map_identity_info(user_id: UserId) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4);
    bytes.put_u32_le(user_id);
    bytes
}

pub fn map_raw_pat(token: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + token.len());
    bytes.put_u8(token.len() as u8);
    bytes.extend(token.as_bytes());
    bytes
}

pub fn map_personal_access_tokens(personal_access_tokens: &[PersonalAccessToken]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for personal_access_token in personal_access_tokens {
        extend_pat(personal_access_token, &mut bytes);
    }
    bytes
}

pub fn map_polled_messages(polled_messages: &PolledMessages) -> Vec<u8> {
    let messages_count = polled_messages.messages.len() as u32;
    let messages_size = polled_messages
        .messages
        .iter()
        .map(|message| message.get_size_bytes())
        .sum::<u32>();

    let mut bytes = Vec::with_capacity(20 + messages_size as usize);
    bytes.put_u32_le(polled_messages.partition_id);
    bytes.put_u64_le(polled_messages.current_offset);
    bytes.put_u32_le(messages_count);
    for message in polled_messages.messages.iter() {
        message.extend(&mut bytes);
    }

    bytes
}

pub async fn map_stream(stream: &Stream) -> Vec<u8> {
    let mut bytes = Vec::new();
    extend_stream(stream, &mut bytes).await;
    for topic in stream.get_topics() {
        extend_topic(topic, &mut bytes).await;
    }
    bytes
}

pub async fn map_streams(streams: &[&Stream]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for stream in streams {
        extend_stream(stream, &mut bytes).await;
    }
    bytes
}

pub async fn map_topics(topics: &[&Topic]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for topic in topics {
        extend_topic(topic, &mut bytes).await;
    }
    bytes
}

pub async fn map_topic(topic: &Topic) -> Vec<u8> {
    let mut bytes = Vec::new();
    extend_topic(topic, &mut bytes).await;
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
        bytes.put_u32_le(member.id);
        let partitions = member.get_partitions();
        bytes.put_u32_le(partitions.len() as u32);
        for partition in partitions {
            bytes.put_u32_le(partition);
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

async fn extend_stream(stream: &Stream, bytes: &mut Vec<u8>) {
    bytes.put_u32_le(stream.stream_id);
    bytes.put_u64_le(stream.created_at);
    bytes.put_u32_le(stream.get_topics().len() as u32);
    bytes.put_u64_le(stream.get_size_bytes().await);
    bytes.put_u64_le(stream.get_messages_count().await);
    bytes.put_u8(stream.name.len() as u8);
    bytes.extend(stream.name.as_bytes());
}

async fn extend_topic(topic: &Topic, bytes: &mut Vec<u8>) {
    bytes.put_u32_le(topic.topic_id);
    bytes.put_u64_le(topic.created_at);
    bytes.put_u32_le(topic.get_partitions().len() as u32);
    match topic.message_expiry {
        Some(message_expiry) => bytes.put_u32_le(message_expiry),
        None => bytes.put_u32_le(0),
    };
    bytes.put_u64_le(topic.get_size_bytes().await);
    bytes.put_u64_le(topic.get_messages_count().await);
    bytes.put_u8(topic.name.len() as u8);
    bytes.extend(topic.name.as_bytes());
}

fn extend_partition(partition: &Partition, bytes: &mut Vec<u8>) {
    bytes.put_u32_le(partition.partition_id);
    bytes.put_u64_le(partition.created_at);
    bytes.put_u32_le(partition.get_segments().len() as u32);
    bytes.put_u64_le(partition.current_offset);
    bytes.put_u64_le(partition.get_size_bytes());
    bytes.put_u64_le(partition.get_messages_count());
}

fn extend_consumer_group(consumer_group: &ConsumerGroup, bytes: &mut Vec<u8>) {
    bytes.put_u32_le(consumer_group.consumer_group_id);
    bytes.put_u32_le(consumer_group.partitions_count);
    bytes.put_u32_le(consumer_group.get_members().len() as u32);
    bytes.put_u8(consumer_group.name.len() as u8);
    bytes.extend(consumer_group.name.as_bytes());
}

fn extend_client(client: &Client, bytes: &mut Vec<u8>) {
    bytes.put_u32_le(client.client_id);
    bytes.put_u32_le(client.user_id.unwrap_or(0));
    let transport: u8 = match client.transport {
        Transport::Tcp => 1,
        Transport::Quic => 2,
    };
    bytes.put_u8(transport);
    let address = client.address.to_string();
    bytes.put_u32_le(address.len() as u32);
    bytes.extend(address.as_bytes());
    bytes.put_u32_le(client.consumer_groups.len() as u32);
}

fn extend_user(user: &User, bytes: &mut Vec<u8>) {
    bytes.put_u32_le(user.id);
    bytes.put_u64(user.created_at);
    bytes.put_u8(user.status.as_code());
    bytes.put_u8(user.username.len() as u8);
    bytes.extend(user.username.as_bytes());
}

fn extend_pat(personal_access_token: &PersonalAccessToken, bytes: &mut Vec<u8>) {
    bytes.put_u8(personal_access_token.name.len() as u8);
    bytes.extend(personal_access_token.name.as_bytes());
    bytes.put_u64_le(personal_access_token.expiry.unwrap_or(0));
}
