use thiserror::Error;
use tokio::io;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error")]
    IoError(#[from] io::Error),
    #[error("Empty response")]
    EmptyResponse,
    #[error("Invalid response: {0}")]
    InvalidResponse(u8),
    #[error("Invalid stream name")]
    InvalidStreamName,
    #[error("Invalid topic name")]
    InvalidTopicName,
    #[error("Too many partitions")]
    TooManyPartitions,
    #[error("Too big payload")]
    TooBigPayload,
}
