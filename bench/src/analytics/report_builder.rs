use super::metrics::group::{from_individual_metrics, from_producers_and_consumers_statistics};
use chrono::{DateTime, Utc};
use iggy::utils::timestamp::IggyTimestamp;
use iggy_benchmark_report::{
    actor_kind::ActorKind, benchmark_kind::BenchmarkKind, hardware::BenchmarkHardware,
    individual_metrics::BenchmarkIndividualMetrics, params::BenchmarkParams,
    report::BenchmarkReport,
};

pub struct BenchmarkReportBuilder;

impl BenchmarkReportBuilder {
    pub fn build(
        server_version: String,
        hardware: BenchmarkHardware,
        params: BenchmarkParams,
        mut individual_metrics: Vec<BenchmarkIndividualMetrics>,
        moving_average_window: u32,
    ) -> BenchmarkReport {
        let uuid = uuid::Uuid::new_v4();

        let timestamp =
            DateTime::<Utc>::from_timestamp_micros(IggyTimestamp::now().as_micros() as i64)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| String::from("unknown"));

        let mut group_metrics = Vec::new();

        // Sort metrics by actor type and ID
        individual_metrics.sort_by_key(|m| (m.summary.actor_kind, m.summary.actor_id));

        // Split metrics by actor type
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

        // Only add aggregate metrics for send and poll benchmark
        if params.benchmark_kind == BenchmarkKind::SendAndPoll
            && !producer_metrics.is_empty()
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
            server_version,
            timestamp,
            hardware,
            params,
            group_metrics,
            individual_metrics,
        }
    }
}
