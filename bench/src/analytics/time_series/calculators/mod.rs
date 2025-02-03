mod latency;
mod throughput;

use iggy::utils::duration::IggyDuration;
use iggy_bench_report::time_series::TimeSeries;
pub use latency::LatencyTimeSeriesCalculator;
pub use throughput::{
    MBThroughputCalculator, MessageThroughputCalculator, ThroughputTimeSeriesCalculator,
};

use crate::analytics::record::BenchmarkRecord;

/// Common functionality for time series calculations
pub trait TimeSeriesCalculation {
    fn calculate(&self, records: &[BenchmarkRecord], bucket_size: IggyDuration) -> TimeSeries;
}
