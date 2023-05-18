use std::time::Duration;

pub struct TestResult {
    pub duration: Duration,
    pub average_latency: f64,
    pub total_size_bytes: u64,
}
