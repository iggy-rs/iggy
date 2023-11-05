use crate::error::Error;
use thiserror::Error;
use tokio::io;

/// The error type for the client.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Command is invalid and cannot be sent.
    #[error("Invalid command")]
    InvalidCommand,
    /// Transport is invalid and cannot be used.
    #[error("Invalid transport {0}")]
    InvalidTransport(String),
    /// IO error.
    #[error("IO error")]
    IoError(#[from] io::Error),
    /// SDK error.
    #[error("SDK error")]
    SdkError(#[from] Error),
}
