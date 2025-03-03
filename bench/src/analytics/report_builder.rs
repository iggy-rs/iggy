use std::collections::HashMap;

use super::metrics::group::{from_individual_metrics, from_producers_and_consumers_statistics};
use crate::utils::get_server_stats;
use chrono::{DateTime, Utc};
use iggy::{
    models::stats::{CacheMetrics, CacheMetricsKey, Stats},
    utils::timestamp::IggyTimestamp,
};
use iggy_bench_report::{
    actor_kind::ActorKind,
    benchmark_kind::BenchmarkKind,
    hardware::BenchmarkHardware,
    individual_metrics::BenchmarkIndividualMetrics,
    params::BenchmarkParams,
    report::BenchmarkReport,
    server_stats::{BenchmarkCacheMetrics, BenchmarkCacheMetricsKey, BenchmarkServerStats},
};

pub struct BenchmarkReportBuilder;

impl BenchmarkReportBuilder {
    pub async fn build(
        hardware: BenchmarkHardware,
        mut params: BenchmarkParams,
        mut individual_metrics: Vec<BenchmarkIndividualMetrics>,
        moving_average_window: u32,
    ) -> BenchmarkReport {
        let uuid = uuid::Uuid::new_v4();

        let timestamp =
            DateTime::<Utc>::from_timestamp_micros(IggyTimestamp::now().as_micros() as i64)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| String::from("unknown"));

        let transport = params.transport;
        let server_addr = params.server_address.clone();

        let server_stats = get_server_stats(&transport, &server_addr)
            .await
            .expect("Failed to get server stats");

        if params.gitref.is_none() {
            params.gitref = Some(server_stats.iggy_server_version.clone());
        };

        if params.gitref_date.is_none() {
            params.gitref_date = Some(timestamp.clone());
        }

        let mut group_metrics = Vec::new();

        individual_metrics.sort_by_key(|m| (m.summary.actor_kind, m.summary.actor_id));

        let producer_metrics: Vec<BenchmarkIndividualMetrics> = individual_metrics
            .iter()
            .filter(|m| m.summary.actor_kind == ActorKind::Producer)
            .cloned()
            .collect();
        let consumer_metrics: Vec<BenchmarkIndividualMetrics> = individual_metrics
            .iter()
            .filter(|m| m.summary.actor_kind == ActorKind::Consumer)
            .cloned()
            .collect();
        let producing_consumers_metrics: Vec<BenchmarkIndividualMetrics> = individual_metrics
            .iter()
            .filter(|m| m.summary.actor_kind == ActorKind::ProducingConsumer)
            .cloned()
            .collect();

        if !producer_metrics.is_empty() {
            if let Some(metrics) = from_individual_metrics(&producer_metrics, moving_average_window)
            {
                group_metrics.push(metrics);
            }
        }

        if !consumer_metrics.is_empty() {
            if let Some(metrics) = from_individual_metrics(&consumer_metrics, moving_average_window)
            {
                group_metrics.push(metrics);
            }
        }

        if !producing_consumers_metrics.is_empty() {
            if let Some(metrics) =
                from_individual_metrics(&producing_consumers_metrics, moving_average_window)
            {
                group_metrics.push(metrics);
            }
        }

        if matches!(
            params.benchmark_kind,
            BenchmarkKind::PinnedProducerAndConsumer
                | BenchmarkKind::BalancedProducerAndConsumerGroup
        ) && !producer_metrics.is_empty()
            && !consumer_metrics.is_empty()
        {
            if let Some(metrics) = from_producers_and_consumers_statistics(
                &producer_metrics,
                &consumer_metrics,
                moving_average_window,
            ) {
                group_metrics.push(metrics);
            }
        }

        BenchmarkReport {
            uuid,
            server_stats: stats_to_benchmark_server_stats(server_stats),
            timestamp,
            hardware,
            params,
            group_metrics,
            individual_metrics,
        }
    }
}

/// This function is a workaround.
/// See server_stats.rs in `iggy_bench_report` crate for more details.
fn stats_to_benchmark_server_stats(stats: Stats) -> BenchmarkServerStats {
    BenchmarkServerStats {
        process_id: stats.process_id,
        cpu_usage: stats.cpu_usage,
        total_cpu_usage: stats.total_cpu_usage,
        memory_usage: stats.memory_usage.as_bytes_u64(),
        total_memory: stats.total_memory.as_bytes_u64(),
        available_memory: stats.available_memory.as_bytes_u64(),
        run_time: stats.run_time.into(),
        start_time: stats.start_time.into(),
        read_bytes: stats.read_bytes.as_bytes_u64(),
        written_bytes: stats.written_bytes.as_bytes_u64(),
        messages_size_bytes: stats.messages_size_bytes.as_bytes_u64(),
        streams_count: stats.streams_count,
        topics_count: stats.topics_count,
        partitions_count: stats.partitions_count,
        segments_count: stats.segments_count,
        messages_count: stats.messages_count,
        clients_count: stats.clients_count,
        consumer_groups_count: stats.consumer_groups_count,
        hostname: stats.hostname,
        os_name: stats.os_name,
        os_version: stats.os_version,
        kernel_version: stats.kernel_version,
        iggy_server_version: stats.iggy_server_version,
        iggy_server_semver: stats.iggy_server_semver,
        cache_metrics: cache_metrics_to_benchmark_cache_metrics(stats.cache_metrics),
    }
}

/// This function is a workaround.
/// See server_stats.rs in `iggy_bench_report` crate for more details.
fn cache_metrics_to_benchmark_cache_metrics(
    cache_metrics: HashMap<CacheMetricsKey, CacheMetrics>,
) -> HashMap<BenchmarkCacheMetricsKey, BenchmarkCacheMetrics> {
    cache_metrics
        .into_iter()
        .map(|(k, v)| {
            (
                BenchmarkCacheMetricsKey {
                    stream_id: k.stream_id,
                    topic_id: k.topic_id,
                    partition_id: k.partition_id,
                },
                BenchmarkCacheMetrics {
                    hits: v.hits,
                    misses: v.misses,
                    hit_ratio: v.hit_ratio,
                },
            )
        })
        .collect()
}
