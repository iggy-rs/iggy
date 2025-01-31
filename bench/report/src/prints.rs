use colored::{Color, ColoredString, Colorize};
use tracing::info;

use crate::{
    group_metrics::BenchmarkGroupMetrics, group_metrics_kind::GroupMetricsKind,
    report::BenchmarkReport,
};

impl BenchmarkReport {
    pub fn print_summary(&self) {
        let kind = self.params.benchmark_kind;
        let total_messages: u64 =
            self.params.messages_per_batch as u64 * self.params.message_batches as u64;
        let total_size_bytes: u64 = total_messages * self.params.message_size as u64;
        let streams = self.params.streams;
        let messages_per_batch = self.params.messages_per_batch;
        let message_batches = self.params.message_batches;
        let message_size = self.params.message_size;
        let producers = self.params.producers;
        let consumers = self.params.consumers;
        println!();
        let params_print = format!("Benchmark: {}, total messages: {}, total size: {} bytes, {} streams, {} messages per batch, {} batches, {} bytes per message, {} producers, {} consumers\n",
            kind,
            total_messages,
            total_size_bytes,
            streams,
            messages_per_batch,
            message_batches,
            message_size,
            producers,
            consumers,
        ).blue();

        info!("{}", params_print);

        self.group_metrics
            .iter()
            .for_each(|s| info!("{}\n", s.formatted_string()));
    }
}

impl BenchmarkGroupMetrics {
    pub fn formatted_string(&self) -> ColoredString {
        let (prefix, color) = match self.summary.kind {
            GroupMetricsKind::Producers => ("Producers Results", Color::Green),
            GroupMetricsKind::Consumers => ("Consumers Results", Color::Green),
            GroupMetricsKind::ProducersAndConsumers => ("Aggregate Results", Color::Red),
        };

        let actor = self.summary.kind.actor();

        let total_mb = format!("{:.2}", self.summary.total_throughput_megabytes_per_second);
        let total_msg = format!("{:.0}", self.summary.total_throughput_messages_per_second);
        let avg_mb = format!(
            "{:.2}",
            self.summary.average_throughput_megabytes_per_second
        );

        let p50 = format!("{:.2}", self.summary.average_p50_latency_ms);
        let p90 = format!("{:.2}", self.summary.average_p90_latency_ms);
        let p95 = format!("{:.2}", self.summary.average_p95_latency_ms);
        let p99 = format!("{:.2}", self.summary.average_p99_latency_ms);
        let p999 = format!("{:.2}", self.summary.average_p999_latency_ms);
        let avg = format!("{:.2}", self.summary.average_latency_ms);
        let median = format!("{:.2}", self.summary.average_median_latency_ms);

        format!(
            "{}: Total throughput: {} MB/s, {} messages/s, average throughput per {}: {} MB/s, \
            p50 latency: {} ms, p90 latency: {} ms, p95 latency: {} ms, \
            p99 latency: {} ms, p999 latency: {} ms, average latency: {} ms, \
            median latency: {} ms",
            prefix, total_mb, total_msg, actor, avg_mb, p50, p90, p95, p99, p999, avg, median,
        )
        .color(color)
    }
}
