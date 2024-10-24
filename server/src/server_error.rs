use error_set::error_set;
use quinn::{ConnectionError as QuicConnectionError, ReadToEndError, WriteError};
use std::array::TryFromSliceError;
use tokio::io;

error_set!(
    ServerError = ConfigError || ArchiverError || ConnectionError || LogError || CompatError;

    IoError = {
        #[display("IO error")]
        IoError(io::Error),

        #[display("Write error")]
        WriteError(WriteError),

        #[display("Read to end error")]
        ReadToEndError(ReadToEndError)
    };

    ConfigError = {
        #[display("Invalid configuration provider: {}", provider_type)]
        InvalidConfigurationProvider { provider_type: String },

        #[display("Cannot load configuration")]
        CannotLoadConfiguration,

        #[display("Invalid configuration")]
        InvalidConfiguration,

        #[display("Cache config validation failure")]
        CacheConfigValidationFailure,
    };

    ArchiverError = {
        #[display("File to archive not found: {}", file_path)]
        FileToArchiveNotFound { file_path: String },

        #[display("Cannot initialize S3 archiver")]
        CannotInitializeS3Archiver,

        #[display("Invalid S3 credentials")]
        InvalidS3Credentials,

        #[display("Cannot archive file: {}", file_path)]
        CannotArchiveFile { file_path: String },
    } || IoError;

    ConnectionError = {
        #[display("Connection error")]
        QuicConnectionError(QuicConnectionError),
    } || IoError || CommonError;

    LogError = {
        #[display("Logging filter reload failure")]
        FilterReloadFailure,

        #[display("Logging stdout reload failure")]
        StdoutReloadFailure,

        #[display("Logging file reload failure")]
        FileReloadFailure,
    };

    CompatError = {
        #[display("Invalid message offset, when performing format conversion")]
        InvalidMessageOffsetFormatConversion,

        #[display("Invalid batch base offset, when performing format conversion")]
        InvalidBatchBaseOffsetFormatConversion,

        #[display("Cannot read message, when performing format conversion")]
        InvalidMessageFieldFormatConversionSampling,

        #[display("Cannot read message batch, when performing format conversion")]
        CannotReadMessageBatchFormatConversion,

        #[display("Index migration error")]
        IndexMigrationError,
    } || IoError || CommonError;

    CommonError = {
        #[display("Try from slice error")]
        TryFromSliceError(TryFromSliceError),

        #[display("SDK error")]
        SdkError(iggy::error::IggyError),
    };
);
