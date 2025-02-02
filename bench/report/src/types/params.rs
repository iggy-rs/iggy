use super::{benchmark_kind::BenchmarkKind, transport::BenchmarkTransport};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct BenchmarkParams {
    pub benchmark_kind: BenchmarkKind,
    pub transport: BenchmarkTransport,
    pub server_address: String,
    pub remark: Option<String>,
    pub extra_info: Option<String>,
    pub gitref: Option<String>,
    pub gitref_date: Option<String>,
    pub messages_per_batch: u32,
    pub message_batches: u32,
    pub message_size: u32,
    pub producers: u32,
    pub consumers: u32,
    pub streams: u32,
    pub partitions: u32,
    pub consumer_groups: u32,
    pub rate_limit: Option<String>,
    pub pretty_name: String,
    pub bench_command: String,
    pub params_identifier: String,
}

impl BenchmarkParams {
    pub fn format_actors_info(&self) -> String {
        match self.benchmark_kind {
            BenchmarkKind::PinnedProducer => format!("{} producers", self.producers),
            BenchmarkKind::PinnedConsumer => format!("{} consumers", self.consumers),
            BenchmarkKind::PinnedProducerAndConsumer => {
                format!("{} producers/{} consumers", self.producers, self.consumers)
            }
            BenchmarkKind::BalancedProducer => format!("{} producers", self.producers),
            BenchmarkKind::BalancedConsumerGroup => format!(
                "{} consumers/{} consumer groups",
                self.consumers, self.consumer_groups
            ),
            BenchmarkKind::BalancedProducerAndConsumerGroup => {
                format!("{} producers/{} consumers", self.producers, self.consumers)
            }
            BenchmarkKind::EndToEndProducingConsumer => {
                format!("{} producing consumers", self.producers)
            } // BenchmarkKind::EndToEndProducerAndConsumerGroup => {
              //     format!(
              //         "{} producers/{} consumers/{} consumer groups",
              //         self.producers, self.consumers, self.consumer_groups
              //     )
              // }
        }
    }
}
