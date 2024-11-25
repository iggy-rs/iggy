use crate::args::simple::BenchmarkKind;
use crate::statistics::actor_statistics::BenchmarkActorStatistics;
use crate::statistics::aggregate_statistics::BenchmarkAggregateStatistics;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct BenchmarkResult {
    pub kind: BenchmarkKind,
    pub statistics: BenchmarkActorStatistics,
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

    fn calculate_statistics<F>(&self, predicate: F) -> Option<BenchmarkAggregateStatistics>
    where
        F: Fn(&BenchmarkResult) -> bool,
    {
        let records: Vec<_> = self
            .results
            .iter()
            .filter(|&result| predicate(result))
            .map(|result| result.statistics.clone())
            .collect();
        BenchmarkAggregateStatistics::from_actors_statistics(&records)
    }

    pub fn dump_to_toml(&self, output_directory: &str) {
        let producer_statics = self.calculate_statistics(|x| x.kind == BenchmarkKind::Send);
        if let Some(producer_statics) = producer_statics {
            let file_path = format!("{}/producers_summary.toml", output_directory);

            producer_statics.dump_to_toml(&file_path);
        }

        let consumer_statics = self.calculate_statistics(|x| x.kind == BenchmarkKind::Poll);
        if let Some(consumer_statics) = consumer_statics {
            let file_path = format!("{}/consumers_summary.toml", output_directory);

            consumer_statics.dump_to_toml(&file_path);
        }
    }
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
