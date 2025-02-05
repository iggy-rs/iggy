use derive_more::Display;
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Display,
    Serialize,
    Deserialize,
    Default,
    PartialOrd,
    Ord,
)]
pub enum BenchmarkKind {
    #[default]
    #[display("Pinned Producer")]
    #[serde(rename = "pinned_producer")]
    PinnedProducer,
    #[display("Pinned Consumer")]
    #[serde(rename = "pinned_consumer")]
    PinnedConsumer,
    #[display("Pinned Producer And Consumer")]
    #[serde(rename = "pinned_producer_and_consumer")]
    PinnedProducerAndConsumer,
    #[display("Balanced Producer")]
    #[serde(rename = "balanced_producer")]
    BalancedProducer,
    #[display("Balanced Consumer Group")]
    #[serde(rename = "balanced_consumer_group")]
    BalancedConsumerGroup,
    #[display("Balanced Producer And Consumer Group")]
    #[serde(rename = "balanced_producer_and_consumer_group")]
    BalancedProducerAndConsumerGroup,
    #[display("End To End Producing Consumer")]
    #[serde(rename = "end_to_end_producing_consumer")]
    EndToEndProducingConsumer,
    #[display("End To End Producing Consumer Group")]
    #[serde(rename = "end_to_end_producing_consumer_group")]
    EndToEndProducingConsumerGroup,
}
