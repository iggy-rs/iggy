use std::io::Write;

use super::actor_statistics::BenchmarkActorStatistics;
use colored::{ColoredString, Colorize};
use serde::Serialize;

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct BenchmarkAggregateStatistics {
    pub total_throughput_megabytes_per_second: f64,
    pub total_throughput_messages_per_second: f64,
    pub average_throughput_megabytes_per_second: f64,
    pub average_throughput_messages_per_second: f64,
    pub average_p50_latency_ms: f64,
    pub average_p90_latency_ms: f64,
    pub average_p95_latency_ms: f64,
    pub average_p99_latency_ms: f64,
    pub average_p999_latency_ms: f64,
    pub average_avg_latency_ms: f64,
    pub average_median_latency_ms: f64,
}

impl BenchmarkAggregateStatistics {
    pub fn from_actors_statistics(stats: &[BenchmarkActorStatistics]) -> Option<Self> {
        if stats.is_empty() {
            return None;
        }
        let count = stats.len() as f64;

        // Compute total throughput
        let total_throughput_megabytes_per_second: f64 = stats
            .iter()
            .map(|r| r.throughput_megabytes_per_second)
            .sum();

        let total_throughput_messages_per_second: f64 =
            stats.iter().map(|r| r.throughput_messages_per_second).sum();

        // Compute average throughput
        let average_throughput_megabytes_per_second = total_throughput_megabytes_per_second / count;

        let average_throughput_messages_per_second = total_throughput_messages_per_second / count;

        // Compute average latencies
        let average_p50_latency_ms = stats.iter().map(|r| r.p50_latency_ms).sum::<f64>() / count;
        let average_p90_latency_ms = stats.iter().map(|r| r.p90_latency_ms).sum::<f64>() / count;
        let average_p95_latency_ms = stats.iter().map(|r| r.p95_latency_ms).sum::<f64>() / count;
        let average_p99_latency_ms = stats.iter().map(|r| r.p99_latency_ms).sum::<f64>() / count;
        let average_p999_latency_ms = stats.iter().map(|r| r.p999_latency_ms).sum::<f64>() / count;
        let average_avg_latency_ms = stats.iter().map(|r| r.avg_latency_ms).sum::<f64>() / count;
        let average_median_latency_ms =
            stats.iter().map(|r| r.median_latency_ms).sum::<f64>() / count;

        Some(BenchmarkAggregateStatistics {
            total_throughput_megabytes_per_second,
            total_throughput_messages_per_second,
            average_throughput_megabytes_per_second,
            average_throughput_messages_per_second,
            average_p50_latency_ms,
            average_p90_latency_ms,
            average_p95_latency_ms,
            average_p99_latency_ms,
            average_p999_latency_ms,
            average_avg_latency_ms,
            average_median_latency_ms,
        })
    }

    pub fn formatted_string(&self, prefix: &str) -> ColoredString {
        format!(
            "{prefix}: Total throughput: {:.2} MB/s, {:.0} messages/s, average throughput: {:.2} MB/s, average p50 latency: {:.2} ms, average p90 latency: {:.2} ms, average p95 latency: {:.2} ms, average p99 latency: {:.2} ms, average p999 latency: {:.2} ms, average latency: {:.2} ms, median latency: {:.2} ms",
            self.total_throughput_megabytes_per_second,
            self.total_throughput_messages_per_second,
            self.average_throughput_megabytes_per_second,
            self.average_p50_latency_ms,
            self.average_p90_latency_ms,
            self.average_p95_latency_ms,
            self.average_p99_latency_ms,
            self.average_p999_latency_ms,
            self.average_avg_latency_ms,
            self.average_median_latency_ms
        ).green()
    }

    pub fn dump_to_toml(&self, file_name: &str) {
        let toml_str = toml::to_string(self).unwrap();
        Write::write_all(
            &mut std::fs::File::create(file_name).unwrap(),
            toml_str.as_bytes(),
        )
        .unwrap();
    }
}
