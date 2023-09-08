use crate::users::permissions::{GlobalPermissions, StreamPermissions};
use crate::users::user::User;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PermissionsValidator {
    enabled: bool,
    users_permissions: HashMap<u32, GlobalPermissions>,
    users_streams_permissions: HashMap<(u32, u32), StreamPermissions>,
    users_that_can_poll_messages_from_all_streams: HashSet<u32>,
    users_that_can_send_messages_to_all_streams: HashSet<u32>,
    users_that_can_poll_messages_from_specific_streams: HashSet<(u32, u32)>,
    users_that_can_send_messages_to_specific_streams: HashSet<(u32, u32)>,
}

impl PermissionsValidator {
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    pub fn disable(&mut self) {
        self.enabled = false;
    }

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
                    .insert((user.id, stream_id), stream);
            }
        }
    }

    pub fn can_poll_messages(&self, user_id: u32, stream_id: u32, topic_id: u32) -> bool {
        if !self.enabled {
            return true;
        }

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

        let stream_permissions = self.users_streams_permissions.get(&(user_id, stream_id));
        if stream_permissions.is_none() {
            return false;
        }

        let stream_permissions = stream_permissions.unwrap();
        if stream_permissions.global.poll_messages {
            return true;
        }

        if stream_permissions.topics.is_none() {
            return false;
        }

        let topic_permissions = stream_permissions.topics.as_ref().unwrap();
        if let Some(topic_permissions) = topic_permissions.get(&topic_id) {
            return topic_permissions.poll_messages;
        }

        false
    }

    pub fn can_send_messages(&self, user_id: u32, stream_id: u32, topic_id: u32) -> bool {
        if !self.enabled {
            return true;
        }

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

        let stream_permissions = self.users_streams_permissions.get(&(user_id, stream_id));
        if stream_permissions.is_none() {
            return false;
        }

        let stream_permissions = stream_permissions.unwrap();
        if stream_permissions.global.send_messages {
            return true;
        }

        if stream_permissions.topics.is_none() {
            return false;
        }

        let topic_permissions = stream_permissions.topics.as_ref().unwrap();
        if let Some(topic_permissions) = topic_permissions.get(&topic_id) {
            return topic_permissions.send_messages;
        }

        false
    }
}
