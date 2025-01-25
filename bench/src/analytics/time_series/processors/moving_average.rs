use super::TimeSeriesProcessor;
use iggy_benchmark_report::time_series::{TimePoint, TimeSeries};
use std::collections::VecDeque;
use tracing::warn;

/// Moving average processor
pub struct MovingAverageProcessor {
    window_size: usize,
}

impl MovingAverageProcessor {
    pub fn new(window_size: usize) -> Self {
        Self { window_size }
    }
}

impl TimeSeriesProcessor for MovingAverageProcessor {
    fn process(&self, data: &TimeSeries) -> TimeSeries {
        if data.points.is_empty() {
            warn!("Attempting to process empty series");
            return data.clone();
        }

        let mut window: VecDeque<f64> = VecDeque::with_capacity(self.window_size);
        let mut points = Vec::with_capacity(data.points.len());

        for point in &data.points {
            window.push_back(point.value);
            if window.len() > self.window_size {
                window.pop_front();
            }

            let avg = window.iter().sum::<f64>() / window.len() as f64;
            let rounded_avg = (avg * 1000.0).round() / 1000.0;
            points.push(TimePoint::new(point.time_s, rounded_avg));
        }

        TimeSeries {
            points,
            kind: data.kind,
        }
    }
}
