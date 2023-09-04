use crate::users::user::User;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize, Deserialize)]
pub struct PermissionsValidator {
    users_permissions: HashMap<u32, GlobalPermissions>,
    users_that_can_poll_messages_from_all_topics: HashSet<u32>,
    users_that_can_send_messages_to_all_topics: HashSet<u32>,
    users_that_can_poll_messages_from_streams: HashSet<(u32, u32)>,
    users_that_can_send_messages_to_streams: HashSet<(u32, u32)>,
    users_streams_permissions: HashMap<(u32, u32), GlobalStreamPermissions>,
    users_topics_permissions: HashMap<(u32, u32, u32), TopicPermissions>,
}

impl PermissionsValidator {
    pub fn new(users: Vec<User>) -> Self {
        let mut users_permissions = HashMap::new();
        let mut users_that_can_poll_messages_from_all_topics = HashSet::new();
        let mut users_that_can_send_messages_to_all_topics = HashSet::new();
        let mut users_that_can_poll_messages_from_streams = HashSet::new();
        let mut users_that_can_send_messages_to_streams = HashSet::new();
        let mut users_streams_permissions = HashMap::new();
        let mut users_topics_permissions = HashMap::new();

        for user in users {
            if user.permissions.is_none() {
                continue;
            }

            let permissions = user.permissions.unwrap();
            if permissions.global.poll_messages {
                users_that_can_poll_messages_from_all_topics.insert(user.id);
            }

            if permissions.global.send_messages {
                users_that_can_send_messages_to_all_topics.insert(user.id);
            }

            users_permissions.insert(user.id, permissions.global);
            if permissions.streams.is_none() {
                continue;
            }

            let streams = permissions.streams.unwrap();
            for (stream_id, stream) in streams {
                if stream.global.poll_messages {
                    users_that_can_poll_messages_from_streams.insert((user.id, stream_id));
                }

                if stream.global.send_messages {
                    users_that_can_send_messages_to_streams.insert((user.id, stream_id));
                }

                users_streams_permissions.insert((user.id, stream_id), stream.global);

                if let Some(topics) = stream.topics {
                    for (topic_id, topic) in topics {
                        users_topics_permissions.insert((user.id, stream_id, topic_id), topic);
                    }
                }
            }
        }

        Self {
            users_streams_permissions,
            users_topics_permissions,
            users_permissions,
            users_that_can_poll_messages_from_all_topics,
            users_that_can_send_messages_to_all_topics,
            users_that_can_poll_messages_from_streams,
            users_that_can_send_messages_to_streams,
        }
    }

    pub fn can_poll_messages(&self, user_id: u32, stream_id: u32, topic_id: u32) -> bool {
        if self
            .users_that_can_poll_messages_from_all_topics
            .contains(&user_id)
        {
            return true;
        }

        if self
            .users_that_can_poll_messages_from_streams
            .contains(&(user_id, stream_id))
        {
            return true;
        }

        let permissions = self
            .users_topics_permissions
            .get(&(user_id, stream_id, topic_id));
        if let Some(permissions) = permissions {
            return permissions.poll_messages;
        }

        false
    }

    pub fn can_send_messages(&self, user_id: u32, stream_id: u32, topic_id: u32) -> bool {
        if self
            .users_that_can_send_messages_to_all_topics
            .contains(&user_id)
        {
            return true;
        }

        if self
            .users_that_can_send_messages_to_streams
            .contains(&(user_id, stream_id))
        {
            return true;
        }

        let permissions = self
            .users_topics_permissions
            .get(&(user_id, stream_id, topic_id));
        if let Some(permissions) = permissions {
            return permissions.send_messages;
        }

        false
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Permissions {
    pub global: GlobalPermissions,
    pub streams: Option<HashMap<u32, StreamPermissions>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalPermissions {
    pub manage_servers: bool,
    pub manage_users: bool,
    pub manage_streams: bool,
    pub manage_topics: bool,
    pub read_streams: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamPermissions {
    pub global: GlobalStreamPermissions,
    pub topics: Option<HashMap<u32, TopicPermissions>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalStreamPermissions {
    pub manage_topics: bool,
    pub read_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicPermissions {
    pub manage_topic: bool,
    pub read_topic: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}
