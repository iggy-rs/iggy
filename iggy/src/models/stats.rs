use serde::{Deserialize, Serialize};

// TODO: Extend with more information.
#[derive(Debug, Serialize, Deserialize)]
pub struct Stats {
    pub streams_count: u32,
    pub topics_count: u32,
    pub partitions_count: u32,
    pub clients_count: u32,
    pub consumer_groups_count: u32,
}
