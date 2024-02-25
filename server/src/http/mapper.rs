use crate::http::jwt::json_web_token::GeneratedTokens;
use crate::streaming::clients::client_manager::Client;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use crate::streaming::users::user::User;
use iggy::locking::IggySharedMut;
use iggy::locking::IggySharedMutFn;
use iggy::models::client_info::ConsumerGroupInfo;
use iggy::models::consumer_group::{ConsumerGroupDetails, ConsumerGroupMember};
use iggy::models::identity_info::{IdentityInfo, IdentityTokens, TokenInfo};
use iggy::models::personal_access_token::PersonalAccessTokenInfo;
use iggy::models::stream::StreamDetails;
use iggy::models::topic::TopicDetails;
use iggy::models::user_info::{UserInfo, UserInfoDetails};
use tokio::sync::RwLock;

pub async fn map_stream(stream: &Stream) -> StreamDetails {
    let topics = map_topics(&stream.get_topics()).await;
    let mut stream_details = StreamDetails {
        id: stream.stream_id,
        created_at: stream.created_at,
        name: stream.name.clone(),
        topics_count: topics.len() as u32,
        size_bytes: stream.get_size(),
        messages_count: stream.get_messages_count(),
        topics,
    };
    stream_details.topics.sort_by(|a, b| a.id.cmp(&b.id));
    stream_details
}

pub async fn map_streams(streams: &[&Stream]) -> Vec<iggy::models::stream::Stream> {
    let mut streams_data = Vec::with_capacity(streams.len());
    for stream in streams {
        let stream = iggy::models::stream::Stream {
            id: stream.stream_id,
            created_at: stream.created_at,
            name: stream.name.clone(),
            size_bytes: stream.get_size(),
            topics_count: stream.get_topics().len() as u32,
            messages_count: stream.get_messages_count(),
        };
        streams_data.push(stream);
    }

    streams_data.sort_by(|a, b| a.id.cmp(&b.id));
    streams_data
}

pub async fn map_topics(topics: &[&Topic]) -> Vec<iggy::models::topic::Topic> {
    let mut topics_data = Vec::with_capacity(topics.len());
    for topic in topics {
        let topic = iggy::models::topic::Topic {
            id: topic.topic_id,
            created_at: topic.created_at,
            name: topic.name.clone(),
            size: topic.get_size(),
            partitions_count: topic.get_partitions().len() as u32,
            messages_count: topic.get_messages_count(),
            message_expiry: topic.message_expiry,
            max_topic_size: topic.max_topic_size,
            replication_factor: topic.replication_factor,
        };
        topics_data.push(topic);
    }
    topics_data.sort_by(|a, b| a.id.cmp(&b.id));
    topics_data
}

pub async fn map_topic(topic: &Topic) -> TopicDetails {
    let mut topic_details = TopicDetails {
        id: topic.topic_id,
        created_at: topic.created_at,
        name: topic.name.clone(),
        size: topic.get_size(),
        messages_count: topic.get_messages_count(),
        partitions_count: topic.get_partitions().len() as u32,
        partitions: Vec::new(),
        message_expiry: topic.message_expiry,
        max_topic_size: topic.max_topic_size,
        replication_factor: topic.replication_factor,
    };
    for partition in topic.get_partitions() {
        let partition = partition.read().await;
        topic_details
            .partitions
            .push(iggy::models::partition::Partition {
                id: partition.partition_id,
                created_at: partition.created_at,
                segments_count: partition.get_segments().len() as u32,
                current_offset: partition.current_offset,
                size_bytes: partition.get_size_bytes().into(),
                messages_count: partition.get_messages_count(),
            });
    }
    topic_details.partitions.sort_by(|a, b| a.id.cmp(&b.id));
    topic_details
}

pub fn map_user(user: &User) -> UserInfoDetails {
    UserInfoDetails {
        id: user.id,
        username: user.username.clone(),
        created_at: user.created_at,
        status: user.status,
        permissions: user.permissions.clone(),
    }
}

pub fn map_users(users: &[User]) -> Vec<UserInfo> {
    let mut users_data = Vec::with_capacity(users.len());
    for user in users {
        let user = UserInfo {
            id: user.id,
            username: user.username.clone(),
            created_at: user.created_at,
            status: user.status,
        };
        users_data.push(user);
    }
    users_data.sort_by(|a, b| a.id.cmp(&b.id));
    users_data
}

pub fn map_personal_access_tokens(
    personal_access_tokens: &[PersonalAccessToken],
) -> Vec<PersonalAccessTokenInfo> {
    let mut personal_access_tokens_data = Vec::with_capacity(personal_access_tokens.len());
    for personal_access_token in personal_access_tokens {
        let personal_access_token = PersonalAccessTokenInfo {
            name: personal_access_token.name.clone(),
            expiry: personal_access_token.expiry,
        };
        personal_access_tokens_data.push(personal_access_token);
    }
    personal_access_tokens_data.sort_by(|a, b| a.name.cmp(&b.name));
    personal_access_tokens_data
}

pub async fn map_client(client: &Client) -> iggy::models::client_info::ClientInfoDetails {
    let client = iggy::models::client_info::ClientInfoDetails {
        client_id: client.client_id,
        user_id: client.user_id,
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
    clients: &[IggySharedMut<Client>],
) -> Vec<iggy::models::client_info::ClientInfo> {
    let mut all_clients = Vec::new();
    for client in clients {
        let client = client.read().await;
        let client = iggy::models::client_info::ClientInfo {
            client_id: client.client_id,
            user_id: client.user_id,
            transport: client.transport.to_string(),
            address: client.address.to_string(),
            consumer_groups_count: client.consumer_groups.len() as u32,
        };
        all_clients.push(client);
    }

    all_clients.sort_by(|a, b| a.client_id.cmp(&b.client_id));
    all_clients
}

pub async fn map_consumer_groups(
    consumer_groups: &[&RwLock<ConsumerGroup>],
) -> Vec<iggy::models::consumer_group::ConsumerGroup> {
    let mut groups = Vec::new();
    for consumer_group in consumer_groups {
        let consumer_group = consumer_group.read().await;
        let consumer_group = iggy::models::consumer_group::ConsumerGroup {
            id: consumer_group.consumer_group_id,
            name: consumer_group.name.clone(),
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
        id: consumer_group.consumer_group_id,
        name: consumer_group.name.clone(),
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

pub fn map_generated_tokens_to_identity_info(tokens: GeneratedTokens) -> IdentityInfo {
    IdentityInfo {
        user_id: tokens.user_id,
        tokens: Some({
            IdentityTokens {
                access_token: TokenInfo {
                    token: tokens.access_token,
                    expiry: tokens.access_token_expiry,
                },
                refresh_token: TokenInfo {
                    token: tokens.refresh_token,
                    expiry: tokens.refresh_token_expiry,
                },
            }
        }),
    }
}
