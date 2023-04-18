use std::array::TryFromSliceError;
use std::num::ParseIntError;
use std::str::Utf8Error;
use thiserror::Error;
use tokio::io;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error")]
    Error,
    #[error("IO error")]
    IoError(#[from] io::Error),
    #[error("Invalid command")]
    InvalidCommand,
    #[error("Invalid command")]
    InvalidFormat,
    #[error("Cannot create base directory")]
    CannotCreateBaseDirectory,
    #[error("Cannot create streams directory")]
    CannotCreateStreamsDirectory,
    #[error("Cannot create stream with ID: {0} directory")]
    CannotCreateStreamDirectory(u32),
    #[error("Failed to create stream info file for stream with ID: {0}")]
    CannotCreateStreamInfo(u32),
    #[error("Failed to update stream info for stream with ID: {0}")]
    CannotUpdateStreamInfo(u32),
    #[error("Failed to open stream info file for stream with ID: {0}")]
    CannotOpenStreamInfo(u32),
    #[error("Failed to read stream info file for stream with ID {0}")]
    CannotReadStreamInfo(u32),
    #[error("Failed to create stream with ID: {0}")]
    CannotCreateStream(u32),
    #[error("Failed to delete stream with ID: {0}")]
    CannotDeleteStream(u32),
    #[error("Failed to delete stream directory with ID: {0}")]
    CannotDeleteStreamDirectory(u32),
    #[error("Stream with ID: {0} was not found.")]
    StreamNotFound(u32),
    #[error("Stream with ID: {0} already exists.")]
    StreamAlreadyExists(u32),
    #[error("Invalid stream name")]
    InvalidStreamName,
    #[error("Cannot create topics directory")]
    CannotCreateTopicsDirectory,
    #[error("Failed to create directory for topic with ID: {0}")]
    CannotCreateTopicDirectory(u32),
    #[error("Failed to create topic info file for topic with ID: {0}")]
    CannotCreateTopicInfo(u32),
    #[error("Failed to update topic info for topic with ID: {0}")]
    CannotUpdateTopicInfo(u32),
    #[error("Failed to open topic info file for topic with ID: {0}")]
    CannotOpenTopicInfo(u32),
    #[error("Failed to read topic info file for topic with ID {0}")]
    CannotReadTopicInfo(u32),
    #[error("Failed to create topic with ID: {0}")]
    CannotCreateTopic(u32),
    #[error("Failed to delete topic with ID: {0}")]
    CannotDeleteTopic(u32),
    #[error("Failed to delete topic directory with ID: {0}")]
    CannotDeleteTopicDirectory(u32),
    #[error("Cannot poll topic")]
    CannotPollTopic,
    #[error("Topic with ID: {0} was not found.")]
    TopicNotFound(u32),
    #[error("Topic with ID: {0} already exists.")]
    TopicAlreadyExists(u32),
    #[error("Invalid topic name")]
    InvalidTopicName,
    #[error("Invalid topic partitions")]
    InvalidTopicPartitions,
    #[error("Log file not found")]
    LogFileNotFound,
    #[error("Cannot append message")]
    CannotAppendMessage,
    #[error("Cannot create partition")]
    CannotCreatePartition,
    #[error("Failed to create directory for partition with ID: {0} for topic with ID: {1}")]
    CannotCreatePartitionDirectory(u32, u32),
    #[error("Failed to create partition segment log file for path: {0}.")]
    CannotCreatePartitionSegmentLogFile(String),
    #[error("Failed to create partition segment index file for path: {0}.")]
    CannotCreatePartitionSegmentIndexFile(String),
    #[error("Failed to create partition segment time index file for path: {0}.")]
    CannotCreatePartitionSegmentTimeIndexFile(String),
    #[error("Cannot open partition log file")]
    CannotOpenPartitionLogFile,
    #[error("Failed to read directory files for topic with ID: {0}")]
    CannotReadPartitions(u32),
    #[error("Partition with ID: {0} was not found.")]
    PartitionNotFound(u32),
    #[error("Invalid messages count")]
    InvalidMessagesCount,
    #[error("Message not found")]
    MessageNotFound,
    #[error("Messages not found")]
    MessagesNotFound,
    #[error("Segment not found")]
    SegmentNotFound,
    #[error("Segment with start offset: {0} and partition ID: {1} is full")]
    SegmentFull(u64, u32),
    #[error("Segment size is invalid")]
    InvalidSegmentSize(u64),
    #[error("Cannot read message")]
    CannotReadMessage,
    #[error("Cannot read message timestamp")]
    CannotReadMessageTimestamp,
    #[error("Cannot read message length")]
    CannotReadMessageLength,
    #[error("Cannot read message payload")]
    CannotReadMessagePayload,
    #[error("Cannot save messages to segment")]
    CannotSaveMessagesToSegment,
    #[error("Cannot save index to segment")]
    CannotSaveIndexToSegment,
    #[error("Cannot save time index to segment")]
    CannotSaveTimeIndexToSegment,
    #[error("Cannot parse integer")]
    CannotParseInt(#[from] ParseIntError),
    #[error("Cannot parse integer")]
    CannotParseSlice(#[from] TryFromSliceError),
    #[error("Cannot parse UTF8")]
    CannotParseUtf8(#[from] Utf8Error),
    #[error("Too big payload")]
    TooBigPayload,
}

// TODO: Categorize errors in the meaningful way.
impl Error {
    pub fn code(&self) -> u8 {
        match self {
            Error::Error => 0,
            Error::IoError(_) => 1,
            Error::InvalidCommand => 2,
            Error::InvalidFormat => 3,
            Error::CannotCreateBaseDirectory => 4,
            Error::CannotCreateStreamsDirectory => 5,
            Error::CannotCreateStreamDirectory(_) => 6,
            Error::CannotCreateStreamInfo(_) => 7,
            Error::CannotUpdateStreamInfo(_) => 8,
            Error::CannotOpenStreamInfo(_) => 9,
            Error::CannotReadStreamInfo(_) => 10,
            Error::CannotCreateStream(_) => 11,
            Error::CannotDeleteStream(_) => 12,
            Error::CannotDeleteStreamDirectory(_) => 13,
            Error::StreamNotFound(_) => 14,
            Error::StreamAlreadyExists(_) => 15,
            Error::InvalidStreamName => 16,
            Error::CannotCreateTopicsDirectory => 17,
            Error::CannotCreateTopicDirectory(_) => 18,
            Error::CannotCreateTopicInfo(_) => 19,
            Error::CannotUpdateTopicInfo(_) => 20,
            Error::CannotOpenTopicInfo(_) => 21,
            Error::CannotReadTopicInfo(_) => 22,
            Error::CannotCreateTopic(_) => 23,
            Error::CannotDeleteTopic(_) => 24,
            Error::CannotDeleteTopicDirectory(_) => 25,
            Error::CannotPollTopic => 26,
            Error::TopicNotFound(_) => 27,
            Error::TopicAlreadyExists(_) => 28,
            Error::InvalidTopicName => 29,
            Error::InvalidTopicPartitions => 30,
            Error::LogFileNotFound => 31,
            Error::CannotAppendMessage => 32,
            Error::CannotCreatePartition => 33,
            Error::CannotCreatePartitionDirectory(_, _) => 34,
            Error::CannotCreatePartitionSegmentLogFile(_) => 35,
            Error::CannotCreatePartitionSegmentIndexFile(_) => 36,
            Error::CannotCreatePartitionSegmentTimeIndexFile(_) => 37,
            Error::CannotOpenPartitionLogFile => 38,
            Error::CannotReadPartitions(_) => 39,
            Error::PartitionNotFound(_) => 40,
            Error::InvalidMessagesCount => 41,
            Error::MessageNotFound => 42,
            Error::MessagesNotFound => 43,
            Error::SegmentNotFound => 44,
            Error::SegmentFull(_, _) => 45,
            Error::InvalidSegmentSize(_) => 46,
            Error::CannotReadMessage => 47,
            Error::CannotReadMessageTimestamp => 48,
            Error::CannotReadMessageLength => 49,
            Error::CannotReadMessagePayload => 50,
            Error::CannotSaveMessagesToSegment => 51,
            Error::CannotSaveIndexToSegment => 52,
            Error::CannotSaveTimeIndexToSegment => 53,
            Error::CannotParseUtf8(_) => 54,
            Error::CannotParseInt(_) => 55,
            Error::CannotParseSlice(_) => 56,
            Error::TooBigPayload => 57,
        }
    }
}
