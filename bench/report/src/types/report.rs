use super::server_stats::BenchmarkServerStats;
use crate::group_metrics::BenchmarkGroupMetrics;
use crate::individual_metrics::BenchmarkIndividualMetrics;
use crate::types::hardware::BenchmarkHardware;
use crate::types::params::BenchmarkParams;
use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkReport {
    /// Benchmark unique identifier
    pub uuid: Uuid,

    /// Timestamp when the benchmark was finished
    pub timestamp: String,

    /// Benchmark server statistics
    pub server_stats: BenchmarkServerStats,

    /// Benchmark hardware
    pub hardware: BenchmarkHardware,

    /// Benchmark parameters
    pub params: BenchmarkParams,

    /// Benchmark metrics for all actors of same type (all producers, all consumers or all actors)
    pub group_metrics: Vec<BenchmarkGroupMetrics>,

    /// Benchmark metrics per actor (producer/consumer)
    pub individual_metrics: Vec<BenchmarkIndividualMetrics>,
}

impl BenchmarkReport {
    pub fn dump_to_json(&self, output_dir: &str) {
        // Create the output directory
        std::fs::create_dir_all(output_dir).expect("Failed to create output directory");

        let report_path = Path::new(output_dir).join("report.json");
        let report_json = serde_json::to_string(self).unwrap();
        std::fs::write(report_path, report_json).expect("Failed to write report to file");
    }
}
