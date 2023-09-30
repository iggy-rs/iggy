use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::system::get_stats::GetStats;
use anyhow::Context;
use async_trait::async_trait;
use byte_unit::Byte;
use chrono::{DateTime, Utc};
use comfy_table::Table;
use humantime::format_duration;
use std::time::{Duration, SystemTime};
use tracing::{event, Level};

pub struct GetStatsCmd {
    get_stats: GetStats,
}

impl GetStatsCmd {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for GetStatsCmd {
    fn default() -> Self {
        Self {
            get_stats: GetStats {},
        }
    }
}

#[async_trait]
impl CliCommand for GetStatsCmd {
    fn explain(&self) -> String {
        "stats command".to_owned()
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let stats = client
            .get_stats(&self.get_stats)
            .await
            .with_context(|| "Problem sending get_stats command".to_owned())?;

        let mut table = Table::new();

        table.set_header(vec!["Server property", "Value"]);
        table.add_row(vec![
            "Iggy Server PID",
            format!("{}", stats.process_id).as_str(),
        ]);
        table.add_row(vec![
            "Iggy Server CPU Usage",
            format!("{:.4} %", stats.cpu_usage).as_str(),
        ]);
        table.add_row(vec![
            "Iggy Server Memory Usage",
            Byte::from_bytes(stats.memory_usage as u128)
                .get_appropriate_unit(true)
                .to_string()
                .as_str(),
        ]);

        table.add_row(vec![
            "Total Memory (RAM)",
            Byte::from_bytes(stats.total_memory as u128)
                .get_appropriate_unit(true)
                .to_string()
                .as_str(),
        ]);
        table.add_row(vec![
            "Available Memory (RAM)",
            Byte::from_bytes(stats.available_memory as u128)
                .get_appropriate_unit(true)
                .to_string()
                .as_str(),
        ]);

        table.add_row(vec![
            "Iggy Server Run Time",
            format!("{}", format_duration(Duration::from_secs(stats.run_time))).as_str(),
        ]);

        let start_time = SystemTime::UNIX_EPOCH + Duration::from_secs(stats.start_time);
        let date_time_utc: DateTime<Utc> = start_time.into();

        table.add_row(vec![
            "Start Time (UTC)",
            format!("{}", date_time_utc.format("%Y-%m-%d %H:%M:%S")).as_str(),
        ]);

        table.add_row(vec![
            "Read Bytes",
            Byte::from_bytes(stats.read_bytes as u128)
                .get_appropriate_unit(true)
                .to_string()
                .as_str(),
        ]);
        table.add_row(vec![
            "Written Bytes",
            Byte::from_bytes(stats.written_bytes as u128)
                .get_appropriate_unit(true)
                .to_string()
                .as_str(),
        ]);
        table.add_row(vec![
            "Messages Size Bytes",
            Byte::from_bytes(stats.messages_size_bytes as u128)
                .get_appropriate_unit(true)
                .to_string()
                .as_str(),
        ]);

        table.add_row(vec![
            "Streams Count",
            format!("{}", stats.streams_count).as_str(),
        ]);
        table.add_row(vec![
            "Topics Count",
            format!("{}", stats.topics_count).as_str(),
        ]);
        table.add_row(vec![
            "Partitions Count",
            format!("{}", stats.partitions_count).as_str(),
        ]);
        table.add_row(vec![
            "Segments Count",
            format!("{}", stats.segments_count).as_str(),
        ]);
        table.add_row(vec![
            "Message Count",
            format!("{}", stats.messages_count).as_str(),
        ]);
        table.add_row(vec![
            "Clients Count",
            format!("{}", stats.clients_count).as_str(),
        ]);
        table.add_row(vec![
            "Consumer Groups Count",
            format!("{}", stats.consumer_groups_count).as_str(),
        ]);

        table.add_row(vec!["OS Name", stats.os_name.as_str()]);
        table.add_row(vec!["OS Version", stats.os_version.as_str()]);
        table.add_row(vec!["Kernel Version", stats.kernel_version.as_str()]);

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
