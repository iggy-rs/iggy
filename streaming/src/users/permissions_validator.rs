use crate::users::permissions::{GlobalPermissions, GlobalStreamPermissions, TopicPermissions};
use crate::users::user::User;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PermissionsValidator {
    users_permissions: HashMap<u32, GlobalPermissions>,
    users_streams_permissions: HashMap<(u32, u32), GlobalStreamPermissions>,
    users_topics_permissions: HashMap<(u32, u32, u32), TopicPermissions>,
    users_that_can_poll_messages_from_all_streams: HashSet<u32>,
    users_that_can_send_messages_to_all_streams: HashSet<u32>,
    users_that_can_poll_messages_from_specific_streams: HashSet<(u32, u32)>,
    users_that_can_send_messages_to_specific_streams: HashSet<(u32, u32)>,
}

impl PermissionsValidator {
    pub fn init(&mut self, users: Vec<User>) {
        for user in users {
            if user.permissions.is_none() {
                continue;
            }

            let permissions = user.permissions.unwrap();
            if permissions.global.poll_messages {
                self.users_that_can_poll_messages_from_all_streams
                    .insert(user.id);
            }

            if permissions.global.send_messages {
                self.users_that_can_send_messages_to_all_streams
                    .insert(user.id);
            }

            self.users_permissions.insert(user.id, permissions.global);
            if permissions.streams.is_none() {
                continue;
            }

            let streams = permissions.streams.unwrap();
            for (stream_id, stream) in streams {
                if stream.global.poll_messages {
                    self.users_that_can_poll_messages_from_specific_streams
                        .insert((user.id, stream_id));
                }

                if stream.global.send_messages {
                    self.users_that_can_send_messages_to_specific_streams
                        .insert((user.id, stream_id));
                }

                self.users_streams_permissions
                    .insert((user.id, stream_id), stream.global);

                if let Some(topics) = stream.topics {
                    for (topic_id, topic) in topics {
                        self.users_topics_permissions
                            .insert((user.id, stream_id, topic_id), topic);
                    }
                }
            }
        }
    }

    pub fn can_poll_messages(&self, user_id: u32, stream_id: u32, topic_id: u32) -> bool {
        if self
            .users_that_can_poll_messages_from_all_streams
            .contains(&user_id)
        {
            return true;
        }

        if self
            .users_that_can_poll_messages_from_specific_streams
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
            .users_that_can_send_messages_to_all_streams
            .contains(&user_id)
        {
            return true;
        }
        if self
            .users_that_can_send_messages_to_specific_streams
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
