use crate::benchmark::BenchmarkKind;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct BenchmarkResult {
    pub kind: BenchmarkKind,
    pub duration: Duration,
    pub average_latency: f64,
    pub total_size_bytes: u64,
}
