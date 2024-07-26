use derive_more::Display;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Display, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticEvent {
    #[display(fmt = "disconnected")]
    Disconnected,
    #[display(fmt = "connected")]
    Connected,
}
