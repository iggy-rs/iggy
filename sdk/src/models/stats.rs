use crate::utils::{byte_size::IggyByteSize, duration::IggyDuration, timestamp::IggyTimestamp};
use serde::{Deserialize, Serialize};

/// `Stats` represents the statistics and details of the server and running process.
#[derive(Debug, Serialize, Deserialize)]
pub struct Stats {
    /// The unique identifier of the process.
    pub process_id: u32,
    /// The CPU usage of the process.
    pub cpu_usage: f32,
    /// the total CPU usage of the system.
    pub total_cpu_usage: f32,
    /// The memory usage of the process.
    pub memory_usage: IggyByteSize,
    /// The total memory of the system.
    pub total_memory: IggyByteSize,
    /// The available memory of the system.
    pub available_memory: IggyByteSize,
    /// The run time of the process.
    pub run_time: IggyDuration,
    /// The start time of the process.
    pub start_time: IggyTimestamp,
    /// The total number of bytes read.
    pub read_bytes: IggyByteSize,
    /// The total number of bytes written.
    pub written_bytes: IggyByteSize,
    /// The total size of the messages in bytes.
    pub messages_size_bytes: IggyByteSize,
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

impl Default for Stats {
    fn default() -> Self {
        Self {
            process_id: 0,
            cpu_usage: 0.0,
            total_cpu_usage: 0.0,
            memory_usage: 0.into(),
            total_memory: 0.into(),
            available_memory: 0.into(),
            run_time: 0.into(),
            start_time: 0.into(),
            read_bytes: 0.into(),
            written_bytes: 0.into(),
            messages_size_bytes: 0.into(),
            streams_count: 0,
            topics_count: 0,
            partitions_count: 0,
            segments_count: 0,
            messages_count: 0,
            clients_count: 0,
            consumer_groups_count: 0,
            hostname: "unknown_hostname".to_string(),
            os_name: "unknown_os_name".to_string(),
            os_version: "unknown_os_version".to_string(),
            kernel_version: "unknown_kernel_version".to_string(),
        }
    }
}
