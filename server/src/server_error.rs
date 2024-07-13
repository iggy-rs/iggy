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
    #[error("Invalid configuration provider: {0}")]
    InvalidConfigurationProvider(String),
    #[error("Cannot load configuration: {0}")]
    CannotLoadConfiguration(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("SDK error")]
    SdkError(#[from] iggy::error::IggyError),
    #[error("Write error")]
    WriteError(#[from] WriteError),
    #[error("Read to end error")]
    ReadToEndError(#[from] ReadToEndError),
    #[error("Try from slice error")]
    TryFromSliceError(#[from] TryFromSliceError),
    #[error("Logging filter reload failure")]
    FilterReloadFailure,
    #[error("Logging stdout reload failure")]
    StdoutReloadFailure,
    #[error("Logging file reload failure")]
    FileReloadFailure,
    #[error("Cache config validation failure: {0}")]
    CacheConfigValidationFailure(String),
    #[error("Command length error: {0}")]
    CommandLengthError(String),
    #[error("Cannot read message, when performing format conversion, {0}")]
    InvalidMessageFieldFormatConversionSampling(String),
    #[error("Invalid message offset, when performing format conversion")]
    InvalidMessageOffsetFormatConversion,
    #[error("Invalid batch base offset, when performing format conversion")]
    InvalidBatchBaseOffsetFormatConversion,
    #[error("Cannot read message batch, when performing format conversion, {0}")]
    CannotReadMessageBatchFormatConversion(String),
    #[error("Cannot remove old segment files")]
    CannotRemoveOldSegmentFiles,
    #[error("Cannot persist new segment files")]
    CannotPersistNewSegmentFiles,
    #[error("Cannot archive file: {0}")]
    CannotArchiveFile(String),
    #[error("Cannot initialize S3 archiver")]
    CannotInitializeS3Archiver,
    #[error("Invalid S3 credentials")]
    InvalidS3Credentials,
    #[error("File to archive not found: {0}")]
    FileToArchiveNotFound(String),
}
