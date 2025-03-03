use super::group_metrics_kind::GroupMetricsKind;
use crate::utils::round_float;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Clone, PartialEq, Deserialize)]
pub struct BenchmarkGroupMetricsSummary {
    pub kind: GroupMetricsKind,
    #[serde(serialize_with = "round_float")]
    pub total_throughput_megabytes_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub total_throughput_messages_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub average_throughput_megabytes_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub average_throughput_messages_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p50_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p90_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p95_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p99_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p999_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p9999_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_median_latency_ms: f64,
    #[serde(default, serialize_with = "round_float")]
    pub min_latency_ms: f64,
    #[serde(default, serialize_with = "round_float")]
    pub max_latency_ms: f64,
    #[serde(default, serialize_with = "round_float")]
    pub std_dev_latency_ms: f64,
}
