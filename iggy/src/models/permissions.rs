use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Permissions {
    pub global: GlobalPermissions,
    pub streams: Option<HashMap<u32, StreamPermissions>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct GlobalPermissions {
    pub manage_servers: bool,
    pub read_servers: bool,
    pub manage_users: bool,
    pub read_users: bool,
    pub manage_streams: bool,
    pub read_streams: bool,
    pub manage_topics: bool,
    pub read_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct StreamPermissions {
    pub manage_stream: bool,
    pub read_stream: bool,
    pub manage_topics: bool,
    pub read_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
    pub topics: Option<HashMap<u32, TopicPermissions>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TopicPermissions {
    pub manage_topic: bool,
    pub read_topic: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

impl Permissions {
    pub fn root() -> Self {
        Self {
            global: GlobalPermissions {
                manage_servers: true,
                read_servers: true,
                manage_users: true,
                read_users: true,
                manage_streams: true,
                read_streams: true,
                manage_topics: true,
                read_topics: true,
                poll_messages: true,
                send_messages: true,
            },
            streams: None,
        }
    }
}

impl Display for Permissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut result = String::new();
        result.push_str(&format!("manage_servers: {}\n", self.global.manage_servers));
        result.push_str(&format!("read_servers: {}\n", self.global.read_servers));
        result.push_str(&format!("manage_users: {}\n", self.global.manage_users));
        result.push_str(&format!("read_users: {}\n", self.global.read_users));
        result.push_str(&format!("manage_streams: {}\n", self.global.manage_streams));
        result.push_str(&format!("read_streams: {}\n", self.global.read_streams));
        result.push_str(&format!("manage_topics: {}\n", self.global.manage_topics));
        result.push_str(&format!("read_topics: {}\n", self.global.read_topics));
        result.push_str(&format!("poll_messages: {}\n", self.global.poll_messages));
        result.push_str(&format!("send_messages: {}\n", self.global.send_messages));
        if let Some(streams) = &self.streams {
            for (stream_id, stream) in streams {
                result.push_str(&format!("stream_id: {}\n", stream_id));
                result.push_str(&format!("manage_stream: {}\n", stream.manage_stream));
                result.push_str(&format!("read_stream: {}\n", stream.read_stream));
                result.push_str(&format!("manage_topics: {}\n", stream.manage_topics));
                result.push_str(&format!("read_topics: {}\n", stream.read_topics));
                result.push_str(&format!("poll_messages: {}\n", stream.poll_messages));
                result.push_str(&format!("send_messages: {}\n", stream.send_messages));
                if let Some(topics) = &stream.topics {
                    for (topic_id, topic) in topics {
                        result.push_str(&format!("topic_id: {}\n", topic_id));
                        result.push_str(&format!("manage_topic: {}\n", topic.manage_topic));
                        result.push_str(&format!("read_topic: {}\n", topic.read_topic));
                        result.push_str(&format!("poll_messages: {}\n", topic.poll_messages));
                        result.push_str(&format!("send_messages: {}\n", topic.send_messages));
                    }
                }
            }
        }

        write!(f, "{}", result)
    }
}
