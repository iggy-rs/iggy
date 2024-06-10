use crate::sourcing::metadata::MetadataEntry;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::*;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::models::permissions::Permissions;
use iggy::models::user_status::UserStatus;
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
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
use std::collections::HashMap;
use std::fmt::Display;
use tracing::{info, warn};

#[derive(Debug)]
pub struct SystemView {
    pub streams: HashMap<Identifier, StreamView>,
    pub users: HashMap<Identifier, UserView>,
}

#[derive(Debug)]
pub struct StreamView {
    pub id: u32,
    pub name: String,
    pub topics: HashMap<Identifier, TopicView>,
    current_topic_id: u32,
}

#[derive(Debug)]
pub struct TopicView {
    pub id: u32,
    pub name: String,
    pub partitions: HashMap<Identifier, PartitionView>,
    pub consumer_groups: HashMap<Identifier, ConsumerGroupView>,
    current_consumer_group_id: u32,
}

#[derive(Debug)]
pub struct PartitionView {
    pub id: u32,
}

#[derive(Debug)]
pub struct UserView {
    pub id: u32,
    pub username: String,
    pub password_hash: String,
    pub status: UserStatus,
    pub permissions: Option<Permissions>,
}

#[derive(Debug)]
pub struct ConsumerGroupView {
    pub id: u32,
    pub name: String,
}

// TODO: Add PATs, also consider handling stream and topic purge
impl SystemView {
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
                    let command = CreateStream::from_bytes(entry.data)?;
                    let stream_id = command.stream_id.unwrap_or_else(|| {
                        current_stream_id += 1;
                        current_stream_id
                    });
                    let stream = StreamView {
                        id: stream_id,
                        name: command.name,
                        topics: HashMap::new(),
                        current_topic_id: 0,
                    };
                    streams.insert(stream.id.try_into()?, stream);
                }
                UPDATE_STREAM_CODE => {
                    let command = UpdateStream::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    stream.name = command.name;
                }
                DELETE_STREAM_CODE => {
                    let command = DeleteStream::from_bytes(entry.data)?;
                    streams.remove(&command.stream_id);
                }
                CREATE_TOPIC_CODE => {
                    let command = CreateTopic::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    let topic_id = command.topic_id.unwrap_or_else(|| {
                        stream.current_topic_id += 1;
                        stream.current_topic_id
                    });
                    let topic = TopicView {
                        id: topic_id,
                        name: command.name,
                        consumer_groups: HashMap::new(),
                        current_consumer_group_id: 0,
                        partitions: if command.partitions_count == 0 {
                            HashMap::new()
                        } else {
                            (1..=command.partitions_count)
                                .map(|id| (id.try_into().unwrap(), PartitionView { id }))
                                .collect()
                        },
                    };
                    stream.topics.insert(topic.id.try_into()?, topic);
                }
                UPDATE_TOPIC_CODE => {
                    let command = UpdateTopic::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    let topic = stream.topics.get_mut(&command.topic_id).unwrap();
                    topic.name = command.name;
                }
                DELETE_TOPIC_CODE => {
                    let command = DeleteTopic::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    stream.topics.remove(&command.topic_id);
                }
                CREATE_PARTITIONS_CODE => {
                    let command = CreatePartitions::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    let topic = stream.topics.get_mut(&command.topic_id).unwrap();
                    let last_partition_id = topic.partitions.values().map(|p| p.id).max().unwrap();
                    for id in last_partition_id + 1..=last_partition_id + command.partitions_count {
                        topic
                            .partitions
                            .insert(id.try_into()?, PartitionView { id });
                    }
                }
                DELETE_PARTITIONS_CODE => {
                    let command = DeletePartitions::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    let topic = stream.topics.get_mut(&command.topic_id).unwrap();
                    let last_partition_id = topic.partitions.values().map(|p| p.id).max().unwrap();
                    for id in last_partition_id - command.partitions_count + 1..=last_partition_id {
                        topic.partitions.remove(&id.try_into().unwrap());
                    }
                }
                CREATE_CONSUMER_GROUP_CODE => {
                    let command = CreateConsumerGroup::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    let topic = stream.topics.get_mut(&command.topic_id).unwrap();
                    let consumer_group_id = command.group_id.unwrap_or_else(|| {
                        topic.current_consumer_group_id += 1;
                        topic.current_consumer_group_id
                    });
                    let consumer_group = ConsumerGroupView {
                        id: consumer_group_id,
                        name: command.name,
                    };
                    topic
                        .consumer_groups
                        .insert(consumer_group.id.try_into()?, consumer_group);
                }
                DELETE_CONSUMER_GROUP_CODE => {
                    let command = DeleteConsumerGroup::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    let topic = stream.topics.get_mut(&command.topic_id).unwrap();
                    topic.consumer_groups.remove(&command.group_id);
                }
                CREATE_USER_CODE => {
                    let command = CreateUser::from_bytes(entry.data)?;
                    current_user_id += 1;
                    let user = UserView {
                        id: current_user_id,
                        username: command.username,
                        password_hash: command.password, // This is already hashed
                        status: command.status,
                        permissions: command.permissions,
                    };
                    users.insert(user.id.try_into()?, user);
                }
                UPDATE_USER_CODE => {
                    let command = UpdateUser::from_bytes(entry.data)?;
                    let user = users.get_mut(&command.user_id).unwrap();
                    if let Some(username) = &command.username {
                        user.username.clone_from(username);
                    }
                    if let Some(status) = &command.status {
                        user.status = *status;
                    }
                }
                DELETE_USER_CODE => {
                    let command = DeleteUser::from_bytes(entry.data)?;
                    users.remove(&command.user_id);
                }
                CHANGE_PASSWORD_CODE => {
                    let command = ChangePassword::from_bytes(entry.data)?;
                    let user = users.get_mut(&command.user_id).unwrap();
                    user.password_hash = command.new_password // This is already hashed
                }
                UPDATE_PERMISSIONS_CODE => {
                    let command = UpdatePermissions::from_bytes(entry.data)?;
                    let user = users.get_mut(&command.user_id).unwrap();
                    user.permissions = command.permissions;
                }
                code => {
                    warn!("Unsupported metadata entry code: {code}");
                }
            }
        }
        Ok(SystemView { streams, users })
    }
}

impl Display for SystemView {
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

impl Display for ConsumerGroupView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConsumerGroup -> ID: {}, Name: {}", self.id, self.name)
    }
}

impl Display for UserView {
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

impl Display for StreamView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stream -> ID: {}, Name: {}", self.id, self.name,)?;
        for topic in self.topics.iter() {
            write!(f, "\n {}", topic.1)?;
        }
        Ok(())
    }
}

impl Display for TopicView {
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

impl Display for PartitionView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Partition -> ID: {}", self.id)
    }
}
