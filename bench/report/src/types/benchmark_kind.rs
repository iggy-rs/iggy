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
    #[display("Send")]
    #[serde(rename = "send")]
    Send,
    #[display("Poll")]
    #[serde(rename = "poll")]
    Poll,
    #[display("Send And Poll")]
    #[serde(rename = "send_and_poll")]
    SendAndPoll,
    #[display("Consumer Group Send")]
    #[serde(rename = "consumer_group_send")]
    ConsumerGroupSend,
    #[display("Consumer Group Poll")]
    #[serde(rename = "consumer_group_poll")]
    ConsumerGroupPoll,
    #[display("Consumer Group Send And Poll")]
    #[serde(rename = "consumer_group_send_and_poll")]
    ConsumerGroupSendAndPoll,
}
