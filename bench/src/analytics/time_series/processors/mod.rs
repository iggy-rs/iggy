use iggy_bench_report::time_series::TimeSeries;

pub mod moving_average;

/// Process time series data
pub trait TimeSeriesProcessor {
    fn process(&self, data: &TimeSeries) -> TimeSeries;
}
