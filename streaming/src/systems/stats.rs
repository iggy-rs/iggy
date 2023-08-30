use crate::systems::system::System;
use iggy::models::stats::Stats;
use sysinfo::{PidExt, ProcessExt, SystemExt};

const PROCESS_NAME: &str = "iggy-server";

impl System {
    pub async fn get_stats(&self) -> Stats {
        let mut sys = sysinfo::System::new_all();
        sys.refresh_system();
        sys.refresh_processes();

        let mut stats = Stats {
            process_id: 0,
            cpu_usage: 0.0,
            memory_usage: 0,
            total_memory: 0,
            available_memory: 0,
            run_time: 0,
            start_time: 0,
            streams_count: self.streams.len() as u32,
            topics_count: self
                .streams
                .values()
                .map(|s| s.topics.len() as u32)
                .sum::<u32>(),
            partitions_count: self
                .streams
                .values()
                .map(|s| {
                    s.topics
                        .values()
                        .map(|t| t.partitions.len() as u32)
                        .sum::<u32>()
                })
                .sum::<u32>(),
            segments_count: 0,
            messages_count: 0,
            clients_count: self.client_manager.read().await.get_clients().len() as u32,
            consumer_groups_count: self
                .streams
                .values()
                .map(|s| {
                    s.topics
                        .values()
                        .map(|t| t.consumer_groups.len() as u32)
                        .sum::<u32>()
                })
                .sum::<u32>(),
            read_bytes: 0,
            written_bytes: 0,
            messages_size_bytes: 0,
            hostname: sys.host_name().unwrap_or("unknown_hostname".to_string()),
            os_name: sys.name().unwrap_or("unknown_os_name".to_string()),
            os_version: sys
                .long_os_version()
                .unwrap_or("unknown_os_version".to_string()),
            kernel_version: sys
                .kernel_version()
                .unwrap_or("unknown_kernel_version".to_string()),
        };

        for (pid, process) in sys.processes() {
            if process.name() != PROCESS_NAME {
                continue;
            }

            stats.process_id = pid.as_u32();
            stats.cpu_usage = process.cpu_usage();
            stats.memory_usage = process.memory();
            stats.total_memory = sys.total_memory();
            stats.available_memory = sys.available_memory();
            stats.run_time = process.run_time();
            stats.start_time = process.start_time();
            let disk_usage = process.disk_usage();
            stats.read_bytes = disk_usage.total_read_bytes;
            stats.written_bytes = disk_usage.total_written_bytes;
            break;
        }

        for stream in self.streams.values() {
            for topic in stream.topics.values() {
                for partition in topic.partitions.values() {
                    let partition = partition.read().await;
                    stats.messages_count += partition.get_messages_count();
                    stats.segments_count += partition.segments.len() as u32;
                    for segment in &partition.segments {
                        stats.messages_size_bytes += segment.current_size_bytes as u64;
                    }
                }
            }
        }

        stats
    }
}
