use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
    pub managed_topics: Option<HashSet<u32>>,
    pub readable_topics: Option<HashSet<u32>>,
    pub poll_messages_from: Option<HashSet<u32>>,
    pub send_messages_to: Option<HashSet<u32>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalStreamPermissions {
    pub manage_topics: bool,
    pub read_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}
