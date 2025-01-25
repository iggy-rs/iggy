use super::{
    individual_metrics_summary::BenchmarkIndividualMetricsSummary, time_series::TimeSeries,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, PartialEq, Deserialize)]
pub struct BenchmarkIndividualMetrics {
    pub summary: BenchmarkIndividualMetricsSummary,
    pub throughput_mb_ts: TimeSeries,
    pub throughput_msg_ts: TimeSeries,
    pub latency_ts: TimeSeries,
}
