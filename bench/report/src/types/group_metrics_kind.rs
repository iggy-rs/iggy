use derive_more::derive::Display;
use serde::{Deserialize, Serialize};

/// The kind of group metrics to be displayed
#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Display)]
pub enum GroupMetricsKind {
    #[display("Producers")]
    #[serde(rename = "producers")]
    Producers,
    #[display("Consumers")]
    #[serde(rename = "consumers")]
    Consumers,
    #[display("Producers and Consumers")]
    #[serde(rename = "producers_and_consumers")]
    ProducersAndConsumers,
    #[display("Producing Consumers")]
    #[serde(rename = "producing_consumers")]
    ProducingConsumers,
}

impl GroupMetricsKind {
    pub fn actor(&self) -> &str {
        match self {
            GroupMetricsKind::Producers => "Producer",
            GroupMetricsKind::Consumers => "Consumer",
            GroupMetricsKind::ProducersAndConsumers => "Actor",
            GroupMetricsKind::ProducingConsumers => "Producing Consumer",
        }
    }
}
