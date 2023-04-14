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
    #[error("Cannot create base directory")]
    CannotCreateBaseDirectory,
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
}

// TODO: Categorize errors in the meaningful way.
impl Error {
    pub fn code(&self) -> u8 {
        match self {
            Error::Error => 0,
            Error::IoError(_) => 1,
            Error::InvalidCommand => 2,
            Error::CannotCreateBaseDirectory => 3,
            Error::CannotCreateTopicsDirectory => 4,
            Error::CannotCreateTopicDirectory(_) => 5,
            Error::CannotCreateTopicInfo(_) => 6,
            Error::CannotUpdateTopicInfo(_) => 7,
            Error::CannotOpenTopicInfo(_) => 8,
            Error::CannotReadTopicInfo(_) => 9,
            Error::CannotCreateTopic(_) => 10,
            Error::CannotDeleteTopic(_) => 11,
            Error::CannotDeleteTopicDirectory(_) => 12,
            Error::CannotPollTopic => 13,
            Error::TopicNotFound(_) => 14,
            Error::TopicAlreadyExists(_) => 15,
            Error::InvalidTopicName => 16,
            Error::InvalidTopicPartitions => 17,
            Error::LogFileNotFound => 18,
            Error::CannotAppendMessage => 19,
            Error::CannotCreatePartition => 20,
            Error::CannotCreatePartitionDirectory(_, _) => 21,
            Error::CannotCreatePartitionSegmentLogFile(_) => 22,
            Error::CannotCreatePartitionSegmentIndexFile(_) => 23,
            Error::CannotCreatePartitionSegmentTimeIndexFile(_) => 24,
            Error::CannotOpenPartitionLogFile => 25,
            Error::CannotReadPartitions(_) => 26,
            Error::PartitionNotFound(_) => 27,
            Error::InvalidMessagesCount => 28,
            Error::MessageNotFound => 29,
            Error::MessagesNotFound => 30,
            Error::SegmentNotFound => 31,
            Error::SegmentFull(_, _) => 32,
            Error::InvalidSegmentSize(_) => 33,
            Error::CannotReadMessage => 34,
            Error::CannotReadMessageTimestamp => 35,
            Error::CannotReadMessageLength => 36,
            Error::CannotReadMessagePayload => 37,
            Error::CannotSaveMessagesToSegment => 38,
            Error::CannotSaveIndexToSegment => 39,
            Error::CannotSaveTimeIndexToSegment => 40,
        }
    }
}
