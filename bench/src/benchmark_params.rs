use crate::args::common::IggyBenchArgs;
use chrono::{DateTime, Utc};
use iggy::utils::timestamp::IggyTimestamp;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct BenchmarkParams {
    pub timestamp: String,
    pub benchmark_kind: String,
    pub transport: String,
    pub pretty_name: Option<String>,
    pub git_ref: Option<String>,
    pub git_ref_date: Option<String>,
    pub messages_per_batch: u32,
    pub message_batches: u32,
    pub message_size: u32,
    pub producers: u32,
    pub consumers: u32,
    pub streams: u32,
    pub partitions: u32,
    pub number_of_consumer_groups: u32,
    pub disable_parallel_consumers: bool,
    pub disable_parallel_producers: bool,
}

impl From<&IggyBenchArgs> for BenchmarkParams {
    fn from(args: &IggyBenchArgs) -> Self {
        let timestamp =
            DateTime::<Utc>::from_timestamp_micros(IggyTimestamp::now().as_micros() as i64)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| String::from("unknown"));

        BenchmarkParams {
            timestamp,
            benchmark_kind: args.benchmark_kind.as_simple_kind().to_string(),
            transport: args.transport().to_string(),
            pretty_name: args.pretty_name(),
            git_ref: args.git_ref(),
            git_ref_date: args.git_ref_date(),
            messages_per_batch: args.messages_per_batch(),
            message_batches: args.message_batches(),
            message_size: args.message_size(),
            producers: args.producers(),
            consumers: args.consumers(),
            streams: args.number_of_streams(),
            partitions: args.number_of_partitions(),
            number_of_consumer_groups: args.number_of_consumer_groups(),
            disable_parallel_consumers: args.disable_parallel_consumer_streams(),
            disable_parallel_producers: args.disable_parallel_producer_streams(),
        }
    }
}
