use crate::args::simple::BenchmarkKind;
use colored::Colorize;
use std::collections::HashSet;
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
    pub latency_percentiles: LatencyPercentiles,
    pub total_size_bytes: u64,
    pub total_messages: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LatencyPercentiles {
    pub p50: Duration,
    pub p90: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub p999: Duration,
}

pub struct BenchmarkResults {
    results: Vec<BenchmarkResult>,
}

impl From<Vec<BenchmarkResult>> for BenchmarkResults {
    fn from(results: Vec<BenchmarkResult>) -> Self {
        Self { results }
    }
}
#[derive(Debug, Clone, PartialEq)]
struct BenchmarkStatistics {
    total_throughput: f64,
    messages_per_second: f64,
    average_p50_latency: f64,
    average_p90_latency: f64,
    average_p95_latency: f64,
    average_p99_latency: f64,
    average_p999_latency: f64,
    average_latency: f64,
    average_throughput: f64,
    total_duration: f64,
}

#[derive(Debug, Clone)]
struct ImpossibleBenchmarkKind;

impl BenchmarkResults {
    fn get_test_type(&self) -> Result<BenchmarkKind, ImpossibleBenchmarkKind> {
        let result_kinds = self
            .results
            .iter()
            .map(|r| r.kind)
            .collect::<HashSet<BenchmarkKind>>();
        match (
            result_kinds.contains(&BenchmarkKind::Poll),
            result_kinds.contains(&BenchmarkKind::Send),
        ) {
            (true, true) => Ok(BenchmarkKind::SendAndPoll),
            (true, false) => Ok(BenchmarkKind::Poll),
            (false, true) => Ok(BenchmarkKind::Send),
            (false, false) => Err(ImpossibleBenchmarkKind),
        }
    }

    fn calculate_statistics<F>(&self, mut predicate: F) -> BenchmarkStatistics
    where
        F: FnMut(&&BenchmarkResult) -> bool,
    {
        let total_size_bytes = self
            .results
            .iter()
            .filter(&mut predicate)
            .map(|r| r.total_size_bytes)
            .sum::<u64>();
        let total_duration = (self
            .results
            .iter()
            .filter(&mut predicate)
            .map(|r| r.end_timestamp - r.start_timestamp)
            .sum::<Duration>()
            / self.results.len() as u32)
            .as_secs_f64();
        let total_messages = self
            .results
            .iter()
            .filter(&mut predicate)
            .map(|r| r.total_messages)
            .sum::<u64>();
        let average_p50_latency = (self
            .results
            .iter()
            .filter(&mut predicate)
            .map(|r| r.latency_percentiles.p50)
            .sum::<Duration>()
            / self.results.len() as u32)
            .as_secs_f64()
            * 1000.0;
        let average_p95_latency = (self
            .results
            .iter()
            .filter(&mut predicate)
            .map(|r| r.latency_percentiles.p95)
            .sum::<Duration>()
            / self.results.len() as u32)
            .as_secs_f64()
            * 1000.0;
        let average_p90_latency = (self
            .results
            .iter()
            .filter(&mut predicate)
            .map(|r| r.latency_percentiles.p90)
            .sum::<Duration>()
            / self.results.len() as u32)
            .as_secs_f64()
            * 1000.0;
        let average_p99_latency = (self
            .results
            .iter()
            .filter(&mut predicate)
            .map(|r| r.latency_percentiles.p99)
            .sum::<Duration>()
            / self.results.len() as u32)
            .as_secs_f64()
            * 1000.0;
        let average_p999_latency = (self
            .results
            .iter()
            .filter(&mut predicate)
            .map(|r| r.latency_percentiles.p999)
            .sum::<Duration>()
            / self.results.len() as u32)
            .as_secs_f64()
            * 1000.0;
        let average_latency = (self
            .results
            .iter()
            .filter(&mut predicate)
            .map(|r| r.average_latency)
            .sum::<Duration>()
            / self.results.len() as u32)
            .as_secs_f64()
            * 1000.0;
        let average_throughput =
            total_size_bytes as f64 / total_duration / 1e6 / self.results.len() as f64;
        let total_throughput = total_size_bytes as f64 / total_duration / 1e6;
        let messages_per_second = total_messages as f64 / total_duration;

        BenchmarkStatistics {
            total_throughput,
            messages_per_second,
            average_latency,
            average_p50_latency,
            average_p90_latency,
            average_p95_latency,
            average_p99_latency,
            average_p999_latency,
            average_throughput,
            total_duration,
        }
    }
}

impl Display for BenchmarkResults {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Ok(test_type) = self.get_test_type() {
            if test_type == BenchmarkKind::SendAndPoll {
                let producer_statics = self.calculate_statistics(|x| x.kind == BenchmarkKind::Send);
                let consumer_statics = self.calculate_statistics(|x| x.kind == BenchmarkKind::Poll);

                let producer_info = format!("Producer results: total throughput: {:.2} MB/s, {:.0} messages/s, average throughput: {:.2} MB/s, average p50 latency: {:.2} ms, average p90 latency: {:.2} ms, average p95 latency: {:.2} ms, average p99 latency: {:.2} ms, average p999 latency: {:.2} ms, average latency: {:.2} ms, total duration: {:.2} s",
                producer_statics.total_throughput, producer_statics.messages_per_second, producer_statics.average_throughput, producer_statics.average_p50_latency, producer_statics.average_p90_latency, producer_statics.average_p95_latency, producer_statics.average_p99_latency, producer_statics.average_p999_latency, producer_statics.average_latency, producer_statics.total_duration).green();

                let consumer_info = format!("Consumer results: total throughput: {:.2} MB/s, {:.0} messages/s, average throughput: {:.2} MB/s, average p50 latency: {:.2} ms, average p90 latency: {:.2} ms, average p95 latency: {:.2} ms, average p99 latency: {:.2} ms, average p999 latency: {:.2} ms, average latency: {:.2} ms, total duration: {:.2} s",
                consumer_statics.total_throughput, consumer_statics.messages_per_second, consumer_statics.average_throughput, consumer_statics.average_p50_latency, consumer_statics.average_p90_latency, consumer_statics.average_p95_latency, consumer_statics.average_p99_latency, consumer_statics.average_p999_latency, consumer_statics.average_latency, consumer_statics.total_duration).green();
                writeln!(f, "{}, {}", producer_info, consumer_info)?;
            }
        }

        let results = self.calculate_statistics(|x| {
            x.kind == BenchmarkKind::Send || x.kind == BenchmarkKind::Poll
        });

        let summary_info = format!("Results: total throughput: {:.2} MB/s, {:.0} messages/s, average throughput: {:.2} MB/s, average p50 latency: {:.2} ms, average p90 latency: {:.2} ms, average p95 latency: {:.2} ms, average p99 latency: {:.2} ms, average p999 latency: {:.2} ms, average latency: {:.2} ms, total duration: {:.2} s",
        results.total_throughput, results.messages_per_second, results.average_throughput, results.average_p50_latency, results.average_p90_latency, results.average_p95_latency, results.average_p99_latency, results.average_p999_latency, results.average_latency, results.total_duration).green();

        writeln!(f, "{}", summary_info)
    }
}
