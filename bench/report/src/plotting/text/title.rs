use crate::{plotting::chart_kind::ChartKind, report::BenchmarkReport};

/// Returns a title for a benchmark report
impl BenchmarkReport {
    pub fn title(&self, kind: ChartKind) -> String {
        if let Some(remark) = &self.params.remark {
            format!(
                "{} - {} Benchmark ({})",
                kind, self.params.benchmark_kind, remark
            )
        } else {
            format!("{} - {} Benchmark", kind, self.params.benchmark_kind)
        }
    }
}
