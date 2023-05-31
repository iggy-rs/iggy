use std::time::Duration;

pub struct BenchmarkResult {
    pub duration: Duration,
    pub average_latency: f64,
    pub total_size_bytes: u64,
}
