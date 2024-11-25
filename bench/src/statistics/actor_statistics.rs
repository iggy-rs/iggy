use super::record::BenchmarkRecord;
use serde::Serialize;

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct BenchmarkActorStatistics {
    pub total_time_secs: f64,
    pub total_user_data_bytes: u64,
    pub total_bytes: u64,
    pub total_messages: u64,
    pub throughput_megabytes_per_second: f64,
    pub throughput_messages_per_second: f64,
    pub p50_latency_ms: f64,
    pub p90_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub p999_latency_ms: f64,
    pub avg_latency_ms: f64,
    pub median_latency_ms: f64,
}

impl BenchmarkActorStatistics {
    pub fn from_records(records: &[BenchmarkRecord]) -> Self {
        if records.is_empty() {
            return BenchmarkActorStatistics {
                total_time_secs: 0.0,
                total_user_data_bytes: 0,
                total_bytes: 0,
                total_messages: 0,
                throughput_megabytes_per_second: 0.0,
                throughput_messages_per_second: 0.0,
                p50_latency_ms: 0.0,
                p90_latency_ms: 0.0,
                p95_latency_ms: 0.0,
                p99_latency_ms: 0.0,
                p999_latency_ms: 0.0,
                avg_latency_ms: 0.0,
                median_latency_ms: 0.0,
            };
        }

        let total_time_secs = records.last().unwrap().elapsed_time_us as f64 / 1_000_000.0;

        let total_user_data_bytes: u64 = records.iter().last().unwrap().user_data_bytes;
        let total_bytes: u64 = records.iter().last().unwrap().total_bytes;
        let total_messages: u64 = records.iter().last().unwrap().messages;

        let throughput_megabytes_per_second = if total_time_secs > 0.0 {
            (total_bytes as f64) / 1_000_000.0 / total_time_secs
        } else {
            0.0
        };

        let throughput_messages_per_second = if total_time_secs > 0.0 {
            (total_messages as f64) / total_time_secs
        } else {
            0.0
        };

        let mut latencies_ms: Vec<f64> = records
            .iter()
            .map(|r| r.latency_us as f64 / 1_000.0)
            .collect();
        latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let p50_latency_ms = calculate_percentile(&latencies_ms, 50.0);
        let p90_latency_ms = calculate_percentile(&latencies_ms, 90.0);
        let p95_latency_ms = calculate_percentile(&latencies_ms, 95.0);
        let p99_latency_ms = calculate_percentile(&latencies_ms, 99.0);
        let p999_latency_ms = calculate_percentile(&latencies_ms, 99.9);

        let avg_latency_ms = latencies_ms.iter().sum::<f64>() / latencies_ms.len() as f64;
        let len = latencies_ms.len() / 2;
        let median_latency_ms = if latencies_ms.len() % 2 == 0 {
            (latencies_ms[len - 1] + latencies_ms[len]) / 2.0
        } else {
            latencies_ms[len]
        };

        BenchmarkActorStatistics {
            total_time_secs,
            total_user_data_bytes,
            total_bytes,
            total_messages,
            throughput_megabytes_per_second,
            throughput_messages_per_second,
            p50_latency_ms,
            p90_latency_ms,
            p95_latency_ms,
            p99_latency_ms,
            p999_latency_ms,
            avg_latency_ms,
            median_latency_ms,
        }
    }

    pub fn dump_to_toml(&self, file_name: &str) {
        let toml_str = toml::to_string(self).unwrap();
        std::fs::write(file_name, toml_str).unwrap();
    }
}

fn calculate_percentile(sorted_data: &[f64], percentile: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }

    let rank = percentile / 100.0 * (sorted_data.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;

    if upper >= sorted_data.len() {
        return sorted_data[sorted_data.len() - 1];
    }

    let weight = rank - lower as f64;
    sorted_data[lower] * (1.0 - weight) + sorted_data[upper] * weight
}
