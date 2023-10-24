use crate::args::simple::BenchmarkKind;
use std::{
    fmt::{Display, Formatter},
    time::Duration,
};
use tokio::time::Instant;

#[derive(Debug, Clone, PartialEq)]
pub struct BenchmarkResult {
    pub kind: BenchmarkKind,
    pub start_timestamp: Instant,
    pub end_timestamp: Instant,
    pub average_latency: Duration,
    pub total_size_bytes: u64,
    pub total_messages: u64,
}

pub struct BenchmarkResults {
    results: Vec<BenchmarkResult>,
}

impl From<Vec<BenchmarkResult>> for BenchmarkResults {
    fn from(results: Vec<BenchmarkResult>) -> Self {
        Self { results }
    }
}

impl Display for BenchmarkResults {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let total_size_bytes = self.results.iter().map(|r| r.total_size_bytes).sum::<u64>();
        let total_duration = (self
            .results
            .iter()
            .map(|r| r.end_timestamp - r.start_timestamp)
            .sum::<Duration>()
            / self.results.len() as u32)
            .as_secs_f64();
        let total_messages = self.results.iter().map(|r| r.total_messages).sum::<u64>();
        let average_latency = (self
            .results
            .iter()
            .map(|r| r.average_latency)
            .sum::<Duration>()
            / self.results.len() as u32)
            .as_secs_f64()
            * 1000.0;
        let average_throughput =
            total_size_bytes as f64 / total_duration / 1024.0 / 1024.0 / self.results.len() as f64;
        let total_throughput = total_size_bytes as f64 / total_duration / 1024.0 / 1024.0;
        let messages_per_second = total_messages as f64 / total_duration;

        write!(
            f,
            "\x1B[32mResults: total throughput: {:.2} MB/s, {:.0} messages/s, average latency: {:.2} ms, average client throughput: {:.2} MB/s, total duration: {:.2} s\x1B[0m",
            total_throughput, messages_per_second, average_latency, average_throughput, total_duration
        )
    }
}
