use colored::{Color, ColoredString, Colorize};
use human_repr::HumanCount;
use tracing::info;

use crate::{
    actor_kind::ActorKind, benchmark_kind::BenchmarkKind, group_metrics::BenchmarkGroupMetrics,
    group_metrics_kind::GroupMetricsKind, report::BenchmarkReport,
};

impl BenchmarkReport {
    pub fn print_summary(&self) {
        let kind = self.params.benchmark_kind;
        let total_messages = format!("{} messages, ", self.total_messages());
        let total_size = format!(
            "{} of data processed",
            self.total_bytes().human_count_bytes()
        );

        let streams = format!("{} streams, ", self.params.streams);
        // TODO: make this configurable
        let topics = "1 topic per stream, ";
        let messages_per_batch = format!("{} messages per batch, ", self.params.messages_per_batch);
        let message_batches = format!("{} message batches, ", self.params.message_batches);
        let message_size = format!("{} bytes per message, ", self.params.message_size);
        let producers = if self.params.producers == 0 {
            "".to_owned()
        } else if self.params.benchmark_kind == BenchmarkKind::EndToEndProducingConsumerGroup
            || self.params.benchmark_kind == BenchmarkKind::EndToEndProducingConsumer
        {
            format!("{} producing consumers, ", self.params.producers)
        } else {
            format!("{} producers, ", self.params.producers)
        };
        let consumers = if self.params.consumers == 0 {
            "".to_owned()
        } else {
            format!("{} consumers, ", self.params.consumers)
        };
        let partitions = if self.params.partitions == 0 {
            "".to_owned()
        } else {
            format!("{} partitions per topic, ", self.params.partitions)
        };
        let consumer_groups = if self.params.consumer_groups == 0 {
            "".to_owned()
        } else {
            format!("{} consumer groups, ", self.params.consumer_groups)
        };
        println!();
        let params_print = format!("Benchmark: {kind}, {producers}{consumers}{streams}{topics}{partitions}{consumer_groups}{total_messages}{messages_per_batch}{message_batches}{message_size}{total_size}\n",).blue();

        info!("{}", params_print);

        self.group_metrics
            .iter()
            .for_each(|s| info!("{}\n", s.formatted_string()));
    }

    pub fn total_messages(&self) -> u64 {
        self.individual_metrics
            .iter()
            .map(|s| s.summary.total_messages)
            .sum()
    }

    pub fn total_messages_sent(&self) -> u64 {
        self.individual_metrics
            .iter()
            .filter(|s| s.summary.actor_kind != ActorKind::Consumer)
            .map(|s| s.summary.total_messages)
            .sum()
    }

    pub fn total_messages_received(&self) -> u64 {
        self.individual_metrics
            .iter()
            .filter(|s| s.summary.actor_kind != ActorKind::Producer)
            .map(|s| s.summary.total_messages)
            .sum()
    }

    pub fn total_bytes_sent(&self) -> u64 {
        self.individual_metrics
            .iter()
            .filter(|s| s.summary.actor_kind != ActorKind::Consumer)
            .map(|s| s.summary.total_user_data_bytes)
            .sum()
    }

    pub fn total_bytes_received(&self) -> u64 {
        self.individual_metrics
            .iter()
            .filter(|s| s.summary.actor_kind != ActorKind::Producer)
            .map(|s| s.summary.total_user_data_bytes)
            .sum()
    }

    pub fn total_bytes(&self) -> u64 {
        self.individual_metrics
            .iter()
            .map(|s| s.summary.total_user_data_bytes)
            .sum()
    }

    pub fn total_message_batches(&self) -> u64 {
        let batches = self
            .individual_metrics
            .iter()
            .map(|s| s.summary.total_message_batches)
            .sum();

        if batches == 0 {
            self.params.message_batches
        } else {
            batches
        }
    }
}

impl BenchmarkGroupMetrics {
    pub fn formatted_string(&self) -> ColoredString {
        let (prefix, color) = match self.summary.kind {
            GroupMetricsKind::Producers => ("Producers Results", Color::Green),
            GroupMetricsKind::Consumers => ("Consumers Results", Color::Green),
            GroupMetricsKind::ProducersAndConsumers => ("Aggregate Results", Color::Red),
            GroupMetricsKind::ProducingConsumers => ("Producing Consumer Results", Color::Red),
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
        let p9999 = format!("{:.2}", self.summary.average_p9999_latency_ms);
        let avg = format!("{:.2}", self.summary.average_latency_ms);
        let median = format!("{:.2}", self.summary.average_median_latency_ms);
        let min = format!("{:.2}", self.summary.min_latency_ms);
        let max = format!("{:.2}", self.summary.max_latency_ms);
        let std_dev = format!("{:.2}", self.summary.std_dev_latency_ms);
        let total_test_time = format!(
            "{:.2}",
            self.avg_throughput_mb_ts.points.last().unwrap().time_s
        );

        format!(
            "{}: Total throughput: {} MB/s, {} messages/s, average throughput per {}: {} MB/s, \
            p50 latency: {} ms, p90 latency: {} ms, p95 latency: {} ms, \
            p99 latency: {} ms, p999 latency: {} ms, p9999 latency: {} ms, average latency: {} ms, \
            median latency: {} ms, min: {} ms, max: {} ms, std dev: {} ms, total time: {} s",
            prefix,
            total_mb,
            total_msg,
            actor,
            avg_mb,
            p50,
            p90,
            p95,
            p99,
            p999,
            p9999,
            avg,
            median,
            min,
            max,
            std_dev,
            total_test_time
        )
        .color(color)
    }
}
