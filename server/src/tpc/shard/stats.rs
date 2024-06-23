use crate::streaming::session::Session;
use fast_async_mutex::mutex::Mutex;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::models::stats::Stats;
use std::sync::OnceLock;
use sysinfo::{Pid, System as SysinfoSystem};

use super::shard::IggyShard;

fn sysinfo() -> &'static Mutex<SysinfoSystem> {
    static SYSINFO: OnceLock<Mutex<SysinfoSystem>> = OnceLock::new();
    SYSINFO.get_or_init(|| {
        let mut sys = SysinfoSystem::new_all();
        sys.refresh_all();
        Mutex::new(sys)
    })
}

impl IggyShard {
    pub async fn get_stats(&self, client_id: u32) -> Result<Stats, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        self.permissioner.borrow().get_stats(user_id)?;
        self.get_stats_bypass_auth().await
    }

    pub async fn get_stats_bypass_auth(&self) -> Result<Stats, IggyError> {
        let mut sys = sysinfo().lock().await;
        let process_id = std::process::id();
        sys.refresh_cpu();
        sys.refresh_memory();
        sys.refresh_process(Pid::from_u32(process_id));

        let total_cpu_usage = sys.global_cpu_info().cpu_usage();
        let total_memory = sys.total_memory().into();
        let available_memory = sys.available_memory().into();
        let clients_count = self.client_manager.borrow().get_clients().len() as u32;
        let hostname = sysinfo::System::host_name().unwrap_or("unknown_hostname".to_string());
        let os_name = sysinfo::System::name().unwrap_or("unknown_os_name".to_string());
        let os_version =
            sysinfo::System::long_os_version().unwrap_or("unknown_os_version".to_string());
        let kernel_version =
            sysinfo::System::kernel_version().unwrap_or("unknown_kernel_version".to_string());

        let mut stats = Stats {
            process_id,
            total_cpu_usage,
            total_memory,
            available_memory,
            clients_count,
            hostname,
            os_name,
            os_version,
            kernel_version,
            ..Default::default()
        };

        if let Some(process) = sys
            .processes()
            .values()
            .find(|p| p.pid() == Pid::from_u32(process_id))
        {
            stats.process_id = process.pid().as_u32();
            stats.cpu_usage = process.cpu_usage();
            stats.memory_usage = process.memory().into();
            stats.run_time = process.run_time().into();
            stats.start_time = process.start_time().into();

            let disk_usage = process.disk_usage();
            stats.read_bytes = disk_usage.total_read_bytes.into();
            stats.written_bytes = disk_usage.total_written_bytes.into();
        }

        drop(sys);

        for stream in self.streams.borrow().values() {
            stats.messages_count += stream.get_messages_count();
            stats.segments_count += stream.get_segments_count();
            stats.messages_size_bytes += stream.get_size();
            stats.streams_count += 1;
            stats.topics_count += stream.topics.len() as u32;
            stats.partitions_count += stream
                .topics
                .values()
                .map(|t| t.partitions.len() as u32)
                .sum::<u32>();
            stats.consumer_groups_count += stream
                .topics
                .values()
                .map(|t| t.consumer_groups.len() as u32)
                .sum::<u32>();
        }

        Ok(stats)
    }
}
