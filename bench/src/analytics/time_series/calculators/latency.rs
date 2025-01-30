use super::TimeSeriesCalculation;
use crate::analytics::record::BenchmarkRecord;
use iggy::utils::duration::IggyDuration;
use iggy_benchmark_report::time_series::{TimePoint, TimeSeries, TimeSeriesKind};
use tracing::warn;

/// Calculator for latency time series
pub struct LatencyTimeSeriesCalculator;

impl TimeSeriesCalculation for LatencyTimeSeriesCalculator {
    // This implementation is using delta latency and average latencies per bucket
    fn calculate(&self, records: &[BenchmarkRecord], bucket_size: IggyDuration) -> TimeSeries {
        if records.len() < 2 {
            warn!("Not enough records to calculate latency");
            return TimeSeries {
                points: Vec::new(),
                kind: TimeSeriesKind::Latency,
            };
        }

        let bucket_size_us = bucket_size.as_micros();

        let max_time_us = records.iter().map(|r| r.elapsed_time_us).max().unwrap();
        let num_buckets = max_time_us.div_ceil(bucket_size_us);
        let mut total_latency_per_bucket = vec![0u64; num_buckets as usize];
        let mut message_count_per_bucket = vec![0u64; num_buckets as usize];

        for window in records.windows(2) {
            let (prev, current) = (&window[0], &window[1]);
            let bucket_index = current.elapsed_time_us / bucket_size_us;
            if bucket_index >= num_buckets {
                continue;
            }

            let delta_messages = current.messages.saturating_sub(prev.messages);
            if delta_messages == 0 {
                continue;
            }

            let delta_latency = current.latency_us.saturating_sub(prev.latency_us);
            total_latency_per_bucket[bucket_index as usize] += delta_latency;
            message_count_per_bucket[bucket_index as usize] += delta_messages;
        }

        let points = (0..num_buckets)
            .filter(|&i| message_count_per_bucket[i as usize] > 0)
            .map(|i| {
                let time_s = (i * bucket_size_us) as f64 / 1_000_000.0;
                let avg_latency_us = total_latency_per_bucket[i as usize] as f64
                    / message_count_per_bucket[i as usize] as f64;
                let rounded_avg_latency_us = (avg_latency_us * 1000.0).round() / 1000.0;
                TimePoint::new(time_s, rounded_avg_latency_us)
            })
            .collect();

        TimeSeries {
            points,
            kind: TimeSeriesKind::Latency,
        }
    }
}
