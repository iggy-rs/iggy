use std::num::ParseIntError;
use thiserror::Error;
use tokio::io;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Invalid command")]
    InvalidCommand,
    #[error("Invalid command parts")]
    InvalidCommandParts,
    #[error("Invalid format")]
    InvalidFormat,
    #[error("IO error")]
    IoError(#[from] io::Error),
    #[error("Cannot parse integer")]
    CannotParseInt(#[from] ParseIntError),
    #[error("SDK error")]
    SdkError(#[from] sdk::error::Error),
}
