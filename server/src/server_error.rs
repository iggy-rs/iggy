use quinn::{ConnectionError, ReadToEndError, WriteError};
use std::array::TryFromSliceError;
use thiserror::Error;
use tokio::io;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("IO error")]
    IoError(#[from] io::Error),
    #[error("Connection error")]
    ConnectionError(#[from] ConnectionError),
    #[error("Cannot load configuration")]
    CannotLoadConfiguration,
    #[error("Invalid configuration")]
    InvalidConfiguration,
    #[error("SDK error")]
    SdkError(#[from] sdk::error::Error),
    #[error("Write error")]
    WriteError(#[from] WriteError),
    #[error("Read to end error")]
    ReadToEndError(#[from] ReadToEndError),
    #[error("Try from slice error")]
    TryFromSliceError(#[from] TryFromSliceError),
}
