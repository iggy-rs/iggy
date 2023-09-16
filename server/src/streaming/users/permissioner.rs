use crate::streaming::users::user::User;
use iggy::models::permissions::{GlobalPermissions, StreamPermissions};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Permissioner {
    pub(super) enabled: bool,
    pub(super) users_permissions: HashMap<u32, GlobalPermissions>,
    pub(super) users_streams_permissions: HashMap<(u32, u32), StreamPermissions>,
    pub(super) users_that_can_poll_messages_from_all_streams: HashSet<u32>,
    pub(super) users_that_can_send_messages_to_all_streams: HashSet<u32>,
    pub(super) users_that_can_poll_messages_from_specific_streams: HashSet<(u32, u32)>,
    pub(super) users_that_can_send_messages_to_specific_streams: HashSet<(u32, u32)>,
}

impl Permissioner {
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    pub fn init(&mut self, users: Vec<User>) {
        for user in users {
            self.init_permissions_for_user(user);
        }
    }

    pub fn init_permissions_for_user(&mut self, user: User) {
        if user.permissions.is_none() {
            return;
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
            return;
        }

        let streams = permissions.streams.unwrap();
        for (stream_id, stream) in streams {
            if stream.poll_messages {
                self.users_that_can_poll_messages_from_specific_streams
                    .insert((user.id, stream_id));
            }

            if stream.send_messages {
                self.users_that_can_send_messages_to_specific_streams
                    .insert((user.id, stream_id));
            }

            self.users_streams_permissions
                .insert((user.id, stream_id), stream);
        }
    }

    pub fn update_permissions_for_user(&mut self, user: User) {
        self.delete_permissions_for_user(user.id);
        self.init_permissions_for_user(user);
    }

    pub fn delete_permissions_for_user(&mut self, user_id: u32) {
        self.users_permissions.remove(&user_id);
        self.users_that_can_poll_messages_from_all_streams
            .remove(&user_id);
        self.users_that_can_send_messages_to_all_streams
            .remove(&user_id);
        self.users_streams_permissions
            .retain(|(id, _), _| *id != user_id);
        self.users_that_can_poll_messages_from_specific_streams
            .retain(|(id, _)| *id != user_id);
        self.users_that_can_send_messages_to_specific_streams
            .retain(|(id, _)| *id != user_id);
    }
}
