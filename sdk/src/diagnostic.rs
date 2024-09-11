use derive_more::Display;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Display, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticEvent {
    #[display("shutdown")]
    Shutdown,
    #[display("disconnected")]
    Disconnected,
    #[display("connected")]
    Connected,
    #[display("signed_in")]
    SignedIn,
    #[display("signed_out")]
    SignedOut,
}
