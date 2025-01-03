use crate::args::simple::BenchmarkKind;
use crate::benchmark_params::BenchmarkParams;
use crate::plotting::generate_plots;
use crate::statistics::actor_statistics::BenchmarkActorSummary;
use crate::statistics::aggregate_statistics::BenchmarkSummary;
use crate::statistics::record::BenchmarkRecord;
use serde::Serialize;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::Path;
use sysinfo::System;

#[derive(Debug, Clone, PartialEq)]
pub struct BenchmarkResult {
    pub kind: BenchmarkKind,
    pub statistics: BenchmarkActorSummary,
}

#[derive(Debug, Clone)]
struct ImpossibleBenchmarkKind;

pub struct BenchmarkResults {
    pub results: Vec<BenchmarkResult>,
}

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

    fn calculate_statistics<F>(&self, predicate: F) -> Option<BenchmarkSummary>
    where
        F: Fn(&BenchmarkResult) -> bool,
    {
        let records: Vec<_> = self
            .results
            .iter()
            .filter(|&result| predicate(result))
            .map(|result| result.statistics.clone())
            .collect();
        BenchmarkSummary::from_actors_statistics(&records)
    }

    pub fn dump_to_json(&self, output_dir: &str, params: BenchmarkParams) {
        let test_type = self.get_test_type().unwrap_or(BenchmarkKind::Send);

        // Get overall statistics for all producers and consumers
        let overall_stats = self.calculate_statistics(|x| {
            x.kind == BenchmarkKind::Send || x.kind == BenchmarkKind::Poll
        });

        // Get first producer statistics and raw data
        let (first_producer_stats, first_producer_raw_data) =
            if test_type == BenchmarkKind::Send || test_type == BenchmarkKind::SendAndPoll {
                if let Some(first_producer) =
                    self.results.iter().find(|x| x.kind == BenchmarkKind::Send)
                {
                    (
                        Some(first_producer.statistics.clone()),
                        Some(first_producer.statistics.raw_data.clone()),
                    )
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

        // Get first consumer statistics and raw data
        let (first_consumer_stats, first_consumer_raw_data) =
            if test_type == BenchmarkKind::Poll || test_type == BenchmarkKind::SendAndPoll {
                if let Some(first_consumer) =
                    self.results.iter().find(|x| x.kind == BenchmarkKind::Poll)
                {
                    (
                        Some(first_consumer.statistics.clone()),
                        Some(first_consumer.statistics.raw_data.clone()),
                    )
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

        let hardware = BenchmarkHardware::new();

        let output = BenchmarkInfo {
            params,
            hardware,
            summary: overall_stats,
            first_producer_summary: first_producer_stats,
            first_consumer_summary: first_consumer_stats,
            first_producer_raw_data,
            first_consumer_raw_data,
        };

        // Create the output directory
        std::fs::create_dir_all(output_dir).expect("Failed to create output directory");

        // Write JSON to data.json in the output directory
        let json_path = Path::new(output_dir).join("data.json");
        let json = serde_json::to_string_pretty(&output).expect("Failed to serialize to JSON");
        fs::write(json_path, json).expect("Failed to write JSON file");

        // Generate plots in the same directory
        generate_plots(&output, output_dir).expect("Failed to generate plots");
    }
}

#[derive(Debug, Serialize)]
pub struct BenchmarkHardware {
    pub cpu_name: String,
    pub cpu_cores: usize,
    pub cpu_frequency_mhz: u64,
    pub total_memory_kb: u64,
    pub hostname: String,
    pub os_name: String,
    pub os_version: String,
}

impl BenchmarkHardware {
    pub fn new() -> Self {
        let mut sys = System::new();
        sys.refresh_all();

        let cpu = sys
            .cpus()
            .first()
            .map(|cpu| (cpu.brand().to_string(), cpu.frequency()))
            .unwrap_or_else(|| (String::from("unknown"), 0));

        Self {
            cpu_name: cpu.0,
            cpu_cores: sys.cpus().len(),
            cpu_frequency_mhz: cpu.1,
            total_memory_kb: sys.total_memory() / 1024, // Convert bytes to KB
            hostname: sysinfo::System::host_name().unwrap_or_else(|| String::from("unknown")),
            os_name: sysinfo::System::name().unwrap_or_else(|| String::from("unknown")),
            os_version: sysinfo::System::kernel_version()
                .unwrap_or_else(|| String::from("unknown")),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct BenchmarkInfo {
    pub params: BenchmarkParams,
    pub hardware: BenchmarkHardware,
    pub summary: Option<BenchmarkSummary>,
    pub first_producer_summary: Option<BenchmarkActorSummary>,
    pub first_consumer_summary: Option<BenchmarkActorSummary>,
    pub first_producer_raw_data: Option<Vec<BenchmarkRecord>>,
    pub first_consumer_raw_data: Option<Vec<BenchmarkRecord>>,
}

impl Display for BenchmarkResults {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Ok(test_type) = self.get_test_type() {
            if test_type == BenchmarkKind::SendAndPoll {
                let producer_statics = self
                    .calculate_statistics(|x| x.kind == BenchmarkKind::Send)
                    .unwrap();
                let consumer_statics = self
                    .calculate_statistics(|x| x.kind == BenchmarkKind::Poll)
                    .unwrap();

                let producer_info = producer_statics.formatted_string("Producer");
                let consumer_info = consumer_statics.formatted_string("Consumer");

                writeln!(f, "{}, {}", producer_info, consumer_info)?;
            }
        }

        let results = self.calculate_statistics(|x| {
            x.kind == BenchmarkKind::Send || x.kind == BenchmarkKind::Poll
        });

        let summary_info = results.unwrap().formatted_string("Results");
        writeln!(f, "{}", summary_info)
    }
}
