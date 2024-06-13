use crate::state::metadata::MetadataEntry;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::*;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::error::IggyError;
use iggy::identifier::{IdKind, Identifier};
use iggy::models::permissions::Permissions;
use iggy::models::user_status::UserStatus;
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::streams::update_stream::UpdateStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::topics::update_topic::UpdateTopic;
use iggy::users::change_password::ChangePassword;
use iggy::users::create_user::CreateUser;
use iggy::users::delete_user::DeleteUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use std::collections::HashMap;
use std::fmt::Display;
use std::str::from_utf8;
use tracing::{debug, info, warn};

#[derive(Debug)]
pub struct SystemState {
    pub streams: HashMap<u32, StreamState>,
    pub users: HashMap<u32, UserState>,
}

#[derive(Debug)]
pub struct StreamState {
    pub id: u32,
    pub name: String,
    pub created_at: IggyTimestamp,
    pub topics: HashMap<u32, TopicState>,
    pub current_topic_id: u32,
}

#[derive(Debug)]
pub struct TopicState {
    pub id: u32,
    pub name: String,
    pub partitions: HashMap<u32, PartitionState>,
    pub consumer_groups: HashMap<u32, ConsumerGroupState>,
    pub compression_algorithm: CompressionAlgorithm,
    pub message_expiry: Option<u32>,
    pub max_topic_size: Option<IggyByteSize>,
    pub replication_factor: Option<u8>,
    pub created_at: IggyTimestamp,
    pub current_consumer_group_id: u32,
}

#[derive(Debug)]
pub struct PartitionState {
    pub id: u32,
}

#[derive(Debug)]
pub struct PersonalAccessTokenState {
    pub name: String,
    pub token_hash: String,
    pub expiry: Option<u64>,
}

#[derive(Debug)]
pub struct UserState {
    pub id: u32,
    pub username: String,
    pub password_hash: String,
    pub status: UserStatus,
    pub permissions: Option<Permissions>,
    pub personal_access_tokens: HashMap<String, PersonalAccessTokenState>,
}

#[derive(Debug)]
pub struct ConsumerGroupState {
    pub id: u32,
    pub name: String,
}

// TODO: Consider handling stream and topic purge
impl SystemState {
    pub async fn init(entries: Vec<MetadataEntry>) -> Result<Self, IggyError> {
        let mut streams = HashMap::new();
        let mut users = HashMap::new();
        let mut current_stream_id = 0;
        let mut current_user_id = 0;
        for entry in entries {
            info!(
                "Processing metadata entry code: {}, name: {}",
                entry.code,
                get_name_from_code(entry.code).unwrap_or("invalid_command")
            );
            match entry.code {
                CREATE_STREAM_CODE => {
                    let command = CreateStream::from_bytes(entry.command)?;
                    let stream_id = command.stream_id.unwrap_or_else(|| {
                        current_stream_id += 1;
                        current_stream_id
                    });
                    let stream = StreamState {
                        id: stream_id,
                        name: command.name.clone(),
                        topics: HashMap::new(),
                        current_topic_id: 0,
                        created_at: entry.timestamp,
                    };
                    streams.insert(stream.id, stream);
                }
                UPDATE_STREAM_CODE => {
                    let command = UpdateStream::from_bytes(entry.command)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams.get_mut(&stream_id).unwrap();
                    stream.name = command.name;
                }
                DELETE_STREAM_CODE => {
                    let command = DeleteStream::from_bytes(entry.command)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    streams.remove(&stream_id);
                }
                CREATE_TOPIC_CODE => {
                    let command = CreateTopic::from_bytes(entry.command)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams.get_mut(&stream_id).unwrap();
                    let topic_id = command.topic_id.unwrap_or_else(|| {
                        stream.current_topic_id += 1;
                        stream.current_topic_id
                    });
                    let topic = TopicState {
                        id: topic_id,
                        name: command.name,
                        consumer_groups: HashMap::new(),
                        current_consumer_group_id: 0,
                        compression_algorithm: command.compression_algorithm,
                        message_expiry: command.message_expiry,
                        max_topic_size: command.max_topic_size,
                        replication_factor: command.replication_factor,
                        created_at: entry.timestamp,
                        partitions: if command.partitions_count == 0 {
                            HashMap::new()
                        } else {
                            (1..=command.partitions_count)
                                .map(|id| (id, PartitionState { id }))
                                .collect()
                        },
                    };
                    stream.topics.insert(topic.id, topic);
                }
                UPDATE_TOPIC_CODE => {
                    let command = UpdateTopic::from_bytes(entry.command)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams.get_mut(&stream_id).unwrap();
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    let topic = stream.topics.get_mut(&topic_id).unwrap();
                    topic.name = command.name;
                    topic.compression_algorithm = command.compression_algorithm;
                    topic.message_expiry = command.message_expiry;
                    topic.max_topic_size = command.max_topic_size;
                    topic.replication_factor = command.replication_factor;
                }
                DELETE_TOPIC_CODE => {
                    let command = DeleteTopic::from_bytes(entry.command)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams.get_mut(&stream_id).unwrap();
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    stream.topics.remove(&topic_id);
                }
                CREATE_PARTITIONS_CODE => {
                    let command = CreatePartitions::from_bytes(entry.command)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams.get_mut(&stream_id).unwrap();
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    let topic = stream.topics.get_mut(&topic_id).unwrap();
                    let last_partition_id = topic.partitions.values().map(|p| p.id).max().unwrap();
                    for id in last_partition_id + 1..=last_partition_id + command.partitions_count {
                        topic.partitions.insert(id, PartitionState { id });
                    }
                }
                DELETE_PARTITIONS_CODE => {
                    let command = DeletePartitions::from_bytes(entry.command)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams.get_mut(&stream_id).unwrap();
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    let topic = stream.topics.get_mut(&topic_id).unwrap();
                    let last_partition_id = topic.partitions.values().map(|p| p.id).max().unwrap();
                    for id in last_partition_id - command.partitions_count + 1..=last_partition_id {
                        topic.partitions.remove(&id);
                    }
                }
                CREATE_CONSUMER_GROUP_CODE => {
                    let command = CreateConsumerGroup::from_bytes(entry.command)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams.get_mut(&stream_id).unwrap();
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    let topic = stream.topics.get_mut(&topic_id).unwrap();
                    let consumer_group_id = command.group_id.unwrap_or_else(|| {
                        topic.current_consumer_group_id += 1;
                        topic.current_consumer_group_id
                    });
                    let consumer_group = ConsumerGroupState {
                        id: consumer_group_id,
                        name: command.name,
                    };
                    topic
                        .consumer_groups
                        .insert(consumer_group.id, consumer_group);
                }
                DELETE_CONSUMER_GROUP_CODE => {
                    let command = DeleteConsumerGroup::from_bytes(entry.command)?;
                    let stream_id = find_stream_id(&streams, &command.stream_id);
                    let stream = streams.get_mut(&stream_id).unwrap();
                    let topic_id = find_topic_id(&stream.topics, &command.topic_id);
                    let topic = stream.topics.get_mut(&topic_id).unwrap();
                    let consumer_group_id =
                        find_consumer_group_id(&topic.consumer_groups, &command.group_id);
                    topic.consumer_groups.remove(&consumer_group_id);
                }
                CREATE_USER_CODE => {
                    let command = CreateUser::from_bytes(entry.command)?;
                    current_user_id += 1;
                    let user = UserState {
                        id: current_user_id,
                        username: command.username,
                        password_hash: command.password, // This is already hashed
                        status: command.status,
                        permissions: command.permissions,
                        personal_access_tokens: HashMap::new(),
                    };
                    users.insert(user.id, user);
                }
                UPDATE_USER_CODE => {
                    let command = UpdateUser::from_bytes(entry.command)?;
                    let user_id = find_user_id(&users, &command.user_id);
                    let user = users.get_mut(&user_id).unwrap();
                    if let Some(username) = &command.username {
                        user.username.clone_from(username);
                    }
                    if let Some(status) = &command.status {
                        user.status = *status;
                    }
                }
                DELETE_USER_CODE => {
                    let command = DeleteUser::from_bytes(entry.command)?;
                    let user_id = find_user_id(&users, &command.user_id);
                    users.remove(&user_id);
                }
                CHANGE_PASSWORD_CODE => {
                    let command = ChangePassword::from_bytes(entry.command)?;
                    let user_id = find_user_id(&users, &command.user_id);
                    let user = users.get_mut(&user_id).unwrap();
                    user.password_hash = command.new_password // This is already hashed
                }
                UPDATE_PERMISSIONS_CODE => {
                    let command = UpdatePermissions::from_bytes(entry.command)?;
                    let user_id = find_user_id(&users, &command.user_id);
                    let user = users.get_mut(&user_id).unwrap();
                    user.permissions = command.permissions;
                }
                CREATE_PERSONAL_ACCESS_TOKEN_CODE => {
                    let command = CreatePersonalAccessToken::from_bytes(entry.command)?;
                    let token_hash = from_utf8(&entry.data)?.to_string();
                    let user_id = find_user_id(&users, &entry.user_id.try_into()?);
                    let user = users.get_mut(&user_id).unwrap();
                    let expiry = command
                        .expiry
                        .map(|e| entry.timestamp.to_micros() + e as u64 * 1_000_000);

                    let now = IggyTimestamp::now().to_micros();
                    if let Some(expiry) = expiry {
                        if expiry < now {
                            debug!("Personal access token: {token_hash} has already expired.");
                            continue;
                        }
                    }

                    user.personal_access_tokens.insert(
                        command.name.clone(),
                        PersonalAccessTokenState {
                            name: command.name,
                            token_hash,
                            expiry,
                        },
                    );
                }
                DELETE_PERSONAL_ACCESS_TOKEN_CODE => {
                    let command = DeletePersonalAccessToken::from_bytes(entry.command)?;
                    let user_id = find_user_id(&users, &entry.user_id.try_into()?);
                    let user = users.get_mut(&user_id).unwrap();
                    user.personal_access_tokens.remove(&command.name);
                }
                code => {
                    warn!("Unsupported metadata entry code: {code}");
                }
            }
        }
        Ok(SystemState { streams, users })
    }
}

fn find_stream_id(streams: &HashMap<u32, StreamState>, stream_id: &Identifier) -> u32 {
    match stream_id.kind {
        IdKind::Numeric => stream_id.get_u32_value().unwrap(),
        IdKind::String => {
            let name = stream_id.get_cow_str_value().unwrap();
            let stream = streams.values().find(|s| s.name == name).unwrap();
            stream.id
        }
    }
}

fn find_topic_id(topics: &HashMap<u32, TopicState>, topic_id: &Identifier) -> u32 {
    match topic_id.kind {
        IdKind::Numeric => topic_id.get_u32_value().unwrap(),
        IdKind::String => {
            let name = topic_id.get_cow_str_value().unwrap();
            let topic = topics.values().find(|s| s.name == name).unwrap();
            topic.id
        }
    }
}

fn find_consumer_group_id(groups: &HashMap<u32, ConsumerGroupState>, group_id: &Identifier) -> u32 {
    match group_id.kind {
        IdKind::Numeric => group_id.get_u32_value().unwrap(),
        IdKind::String => {
            let name = group_id.get_cow_str_value().unwrap();
            let group = groups.values().find(|s| s.name == name).unwrap();
            group.id
        }
    }
}

fn find_user_id(users: &HashMap<u32, UserState>, user_id: &Identifier) -> u32 {
    match user_id.kind {
        IdKind::Numeric => user_id.get_u32_value().unwrap(),
        IdKind::String => {
            let username = user_id.get_cow_str_value().unwrap();
            let user = users.values().find(|s| s.username == username).unwrap();
            user.id
        }
    }
}

impl Display for SystemState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Streams:")?;
        for stream in self.streams.iter() {
            write!(f, "\n================\n")?;
            write!(f, "{}", stream.1)?;
        }
        write!(f, "Users:")?;
        for user in self.users.iter() {
            write!(f, "\n================\n")?;
            write!(f, "{}", user.1)?;
        }
        Ok(())
    }
}

impl Display for ConsumerGroupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConsumerGroup -> ID: {}, Name: {}", self.id, self.name)
    }
}

impl Display for UserState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let permissions = if let Some(permissions) = &self.permissions {
            permissions.to_string()
        } else {
            "no_permissions".to_string()
        };
        write!(
            f,
            "User -> ID: {}, Username: {}, Status: {}, Permissions: {}",
            self.id, self.username, self.status, permissions
        )
    }
}

impl Display for StreamState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stream -> ID: {}, Name: {}", self.id, self.name,)?;
        for topic in self.topics.iter() {
            write!(f, "\n {}", topic.1)?;
        }
        Ok(())
    }
}

impl Display for TopicState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Topic -> ID: {}, Name: {}", self.id, self.name,)?;
        for partition in self.partitions.iter() {
            write!(f, "\n  {}", partition.1)?;
        }
        write!(f, "\nConsumer Groups:")?;
        for consumer_group in self.consumer_groups.iter() {
            write!(f, "\n  {}", consumer_group.1)?;
        }
        Ok(())
    }
}

impl Display for PartitionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Partition -> ID: {}", self.id)
    }
}
