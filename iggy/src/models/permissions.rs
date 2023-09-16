use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use bytes::{Buf, BufMut};
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

impl BytesSerializable for Permissions {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.put_u8(if self.global.manage_servers { 1 } else { 0 });
        bytes.put_u8(if self.global.read_servers { 1 } else { 0 });
        bytes.put_u8(if self.global.manage_users { 1 } else { 0 });
        bytes.put_u8(if self.global.read_users { 1 } else { 0 });
        bytes.put_u8(if self.global.manage_streams { 1 } else { 0 });
        bytes.put_u8(if self.global.read_streams { 1 } else { 0 });
        bytes.put_u8(if self.global.manage_topics { 1 } else { 0 });
        bytes.put_u8(if self.global.read_topics { 1 } else { 0 });
        bytes.put_u8(if self.global.poll_messages { 1 } else { 0 });
        bytes.put_u8(if self.global.send_messages { 1 } else { 0 });
        if let Some(streams) = &self.streams {
            bytes.put_u8(1);
            let streams_count = streams.len();
            let mut current_stream = 1;
            for (stream_id, stream) in streams {
                bytes.put_u32_le(*stream_id);
                bytes.put_u8(if stream.manage_stream { 1 } else { 0 });
                bytes.put_u8(if stream.read_stream { 1 } else { 0 });
                bytes.put_u8(if stream.manage_topics { 1 } else { 0 });
                bytes.put_u8(if stream.read_topics { 1 } else { 0 });
                bytes.put_u8(if stream.poll_messages { 1 } else { 0 });
                bytes.put_u8(if stream.send_messages { 1 } else { 0 });
                if let Some(topics) = &stream.topics {
                    bytes.put_u8(1);
                    let topics_count = topics.len();
                    let mut current_topic = 1;
                    for (topic_id, topic) in topics {
                        bytes.put_u32_le(*topic_id);
                        bytes.put_u8(if topic.manage_topic { 1 } else { 0 });
                        bytes.put_u8(if topic.read_topic { 1 } else { 0 });
                        bytes.put_u8(if topic.poll_messages { 1 } else { 0 });
                        bytes.put_u8(if topic.send_messages { 1 } else { 0 });
                        if current_topic < topics_count {
                            current_topic += 1;
                            bytes.put_u8(1);
                        } else {
                            bytes.put_u8(0);
                        }
                    }
                } else {
                    bytes.put_u8(0);
                }
                if current_stream < streams_count {
                    current_stream += 1;
                    bytes.put_u8(1);
                } else {
                    bytes.put_u8(0);
                }
            }
        } else {
            bytes.put_u8(0);
        }
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let mut bytes = bytes;
        let manage_servers = bytes.get_u8() == 1;
        let read_servers = bytes.get_u8() == 1;
        let manage_users = bytes.get_u8() == 1;
        let read_users = bytes.get_u8() == 1;
        let manage_streams = bytes.get_u8() == 1;
        let read_streams = bytes.get_u8() == 1;
        let manage_topics = bytes.get_u8() == 1;
        let read_topics = bytes.get_u8() == 1;
        let poll_messages = bytes.get_u8() == 1;
        let send_messages = bytes.get_u8() == 1;
        let mut streams = None;
        if bytes.get_u8() == 1 {
            let mut streams_map = HashMap::new();
            loop {
                let stream_id = bytes.get_u32_le();
                let manage_stream = bytes.get_u8() == 1;
                let read_stream = bytes.get_u8() == 1;
                let manage_topics = bytes.get_u8() == 1;
                let read_topics = bytes.get_u8() == 1;
                let poll_messages = bytes.get_u8() == 1;
                let send_messages = bytes.get_u8() == 1;
                let mut topics = None;
                if bytes.get_u8() == 1 {
                    let mut topics_map = HashMap::new();
                    loop {
                        let topic_id = bytes.get_u32_le();
                        let manage_topic = bytes.get_u8() == 1;
                        let read_topic = bytes.get_u8() == 1;
                        let poll_messages = bytes.get_u8() == 1;
                        let send_messages = bytes.get_u8() == 1;
                        topics_map.insert(
                            topic_id,
                            TopicPermissions {
                                manage_topic,
                                read_topic,
                                poll_messages,
                                send_messages,
                            },
                        );
                        if bytes.get_u8() == 0 {
                            break;
                        }
                    }
                    topics = Some(topics_map);
                }
                streams_map.insert(
                    stream_id,
                    StreamPermissions {
                        manage_stream,
                        read_stream,
                        manage_topics,
                        read_topics,
                        poll_messages,
                        send_messages,
                        topics,
                    },
                );
                if bytes.get_u8() == 0 {
                    break;
                }
            }
            streams = Some(streams_map);
        }
        Ok(Self {
            global: GlobalPermissions {
                manage_servers,
                read_servers,
                manage_users,
                read_users,
                manage_streams,
                read_streams,
                manage_topics,
                read_topics,
                poll_messages,
                send_messages,
            },
            streams,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_and_deserialized_from_bytes() {
        let permissions = Permissions {
            global: GlobalPermissions {
                manage_servers: true,
                read_servers: true,
                manage_users: true,
                read_users: true,
                manage_streams: false,
                read_streams: true,
                manage_topics: false,
                read_topics: true,
                poll_messages: true,
                send_messages: true,
            },
            streams: Some(HashMap::from([
                (
                    1,
                    StreamPermissions {
                        manage_stream: true,
                        read_stream: true,
                        manage_topics: true,
                        read_topics: true,
                        poll_messages: true,
                        send_messages: true,
                        topics: Some(HashMap::from([
                            (
                                1,
                                TopicPermissions {
                                    manage_topic: true,
                                    read_topic: true,
                                    poll_messages: true,
                                    send_messages: true,
                                },
                            ),
                            (
                                2,
                                TopicPermissions {
                                    manage_topic: true,
                                    read_topic: false,
                                    poll_messages: true,
                                    send_messages: false,
                                },
                            ),
                        ])),
                    },
                ),
                (
                    2,
                    StreamPermissions {
                        manage_stream: false,
                        read_stream: true,
                        manage_topics: false,
                        read_topics: true,
                        poll_messages: true,
                        send_messages: true,
                        topics: None,
                    },
                ),
            ])),
        };

        let bytes = permissions.as_bytes();
        let deserialized_permissions = Permissions::from_bytes(&bytes).unwrap();

        assert_eq!(permissions, deserialized_permissions);
    }
}
