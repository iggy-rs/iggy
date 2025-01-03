use super::{group_metrics_summary::BenchmarkGroupMetricsSummary, time_series::TimeSeries};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Clone, PartialEq, Deserialize)]
pub struct BenchmarkGroupMetrics {
    pub summary: BenchmarkGroupMetricsSummary,
    pub avg_throughput_mb_ts: TimeSeries,
    pub avg_throughput_msg_ts: TimeSeries,
    pub avg_latency_ts: TimeSeries,
}
