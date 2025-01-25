use crate::{
    benchmark_kind::BenchmarkKind, plotting::chart_kind::ChartKind, report::BenchmarkReport,
};

/// Returns a title for a benchmark report
impl BenchmarkReport {
    pub fn title(&self, kind: ChartKind) -> String {
        let kind_str = match self.params.benchmark_kind {
            BenchmarkKind::Send => "Send",
            BenchmarkKind::Poll => "Poll",
            BenchmarkKind::SendAndPoll => "Send and Poll",
            BenchmarkKind::ConsumerGroupPoll => "Consumer Group Poll",
        };

        if let Some(remark) = &self.params.remark {
            format!("{} - {} Benchmark ({})", kind, kind_str, remark)
        } else {
            format!("{} - {} Benchmark", kind, kind_str)
        }
    }
}
