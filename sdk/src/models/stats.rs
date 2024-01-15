use serde::{Deserialize, Serialize};

/// `Stats` represents the statistics and details of the server and running process.
#[derive(Debug, Serialize, Deserialize)]
pub struct Stats {
    /// The unique identifier of the process.
    pub process_id: u32,
    /// The CPU usage of the process.
    pub cpu_usage: f32,
    /// The memory usage of the process.
    pub memory_usage: u64,
    /// The total memory of the system.
    pub total_memory: u64,
    /// The available memory of the system.
    pub available_memory: u64,
    /// The run time of the process.
    pub run_time: u64,
    /// The start time of the process.
    pub start_time: u64,
    /// The total number of bytes read.
    pub read_bytes: u64,
    /// The total number of bytes written.
    pub written_bytes: u64,
    /// The total size of the messages in bytes.
    pub messages_size_bytes: u64,
    /// The total number of streams.
    pub streams_count: u32,
    /// The total number of topics.
    pub topics_count: u32,
    /// The total number of partitions.
    pub partitions_count: u32,
    /// The total number of segments.
    pub segments_count: u32,
    /// The total number of messages.
    pub messages_count: u64,
    /// The total number of connected clients.
    pub clients_count: u32,
    /// The total number of consumer groups.
    pub consumer_groups_count: u32,
    /// The name of the host.
    pub hostname: String,
    /// The details of the operating system.
    pub os_name: String,
    /// The version of the operating system.
    pub os_version: String,
    /// The version of the kernel.
    pub kernel_version: String,
}
