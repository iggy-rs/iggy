use std::io::Write;

use crate::args::common::IggyBenchArgs;
use iggy::utils::timestamp::IggyTimestamp;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct BenchmarkParams {
    timestamp_micros: i64,
    benchmark_name: String,
    transport: String,
    messages_per_batch: u32,
    message_batches: u32,
    message_size: u32,
    producers: u32,
    consumers: u32,
    streams: u32,
    partitions: u32,
    number_of_consumer_groups: u32,
    disable_parallel_consumers: bool,
    disable_parallel_producers: bool,
}

impl BenchmarkParams {
    pub fn dump_to_toml(&self, output_directory: &str) {
        let output_file = format!("{}/params.toml", output_directory);
        let toml_str = toml::to_string(self).unwrap();
        Write::write_all(
            &mut std::fs::File::create(output_file).unwrap(),
            toml_str.as_bytes(),
        )
        .unwrap();
    }
}

impl From<&IggyBenchArgs> for BenchmarkParams {
    fn from(args: &IggyBenchArgs) -> Self {
        BenchmarkParams {
            timestamp_micros: IggyTimestamp::now().as_micros() as i64,
            benchmark_name: args.benchmark_kind.as_simple_kind().to_string(),
            transport: args.transport().to_string(),
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
