use derive_more::derive::Display;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Display)]
pub enum ActorKind {
    #[display("Producer")]
    #[serde(rename = "producer")]
    Producer,
    #[display("Consumer")]
    #[serde(rename = "consumer")]
    Consumer,
}

impl ActorKind {
    pub fn plural(&self) -> &str {
        match self {
            ActorKind::Producer => "Producers",
            ActorKind::Consumer => "Consumers",
        }
    }
}
