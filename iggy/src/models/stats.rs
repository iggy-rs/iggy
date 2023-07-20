use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Stats {
    pub process_id: u32,
    pub cpu_usage: f32,
    pub memory_usage: u64,
    pub total_memory: u64,
    pub available_memory: u64,
    pub run_time: u64,
    pub start_time: u64,
    pub read_bytes: u64,
    pub written_bytes: u64,
    pub messages_size_bytes: u64,
    pub streams_count: u32,
    pub topics_count: u32,
    pub partitions_count: u32,
    pub segments_count: u32,
    pub messages_count: u64,
    pub clients_count: u32,
    pub consumer_groups_count: u32,
}
