use derive_more::derive::Display;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Display, Default, Hash)]
pub enum BenchmarkTransport {
    #[default]
    #[display("TCP")]
    #[serde(rename = "tcp")]
    Tcp,
    #[display("HTTP")]
    #[serde(rename = "http")]
    Http,
    #[display("QUIC")]
    #[serde(rename = "quic")]
    Quic,
}
