use serde::{Deserialize, Serialize};

/// A point in time series data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TimePoint {
    pub time_s: f64,
    pub value: f64,
}

impl TimePoint {
    pub fn new(time_s: f64, value: f64) -> Self {
        Self { time_s, value }
    }
}

/// Time series data with associated metadata
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TimeSeries {
    pub points: Vec<TimePoint>,
    #[serde(skip)]
    pub kind: TimeSeriesKind,
}

/// Types of time series data we can calculate
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum TimeSeriesKind {
    #[default]
    ThroughputMB,
    ThroughputMsg,
    Latency,
}

impl TimeSeries {
    pub fn as_charming_points(&self) -> Vec<Vec<f64>> {
        self.points
            .iter()
            .map(|p| vec![p.time_s, p.value])
            .collect()
    }
}
