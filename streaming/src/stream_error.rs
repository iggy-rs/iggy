use thiserror::Error;
use tokio::io;

#[derive(Debug, Error)]
pub enum StreamError {
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
    #[error("Segment full")]
    SegmentFull,
    #[error("Cannot read message")]
    CannotReadMessage,
    #[error("Cannot read message timestamp")]
    CannotReadMessageTimestamp,
    #[error("Cannot read message length")]
    CannotReadMessageLength,
    #[error("Cannot read message payload")]
    CannotReadMessagePayload
}

//TODO: Categorize errors in the meaningful way.
impl StreamError {
    pub fn code(&self) -> u8 {
        match self {
            StreamError::Error => 0,
            StreamError::IoError(_) => 1,
            StreamError::InvalidCommand => 2,
            StreamError::CannotCreateBaseDirectory => 3,
            StreamError::CannotCreateTopicsDirectory => 4,
            StreamError::CannotCreateTopicDirectory(_) => 5,
            StreamError::CannotCreateTopicInfo(_) => 6,
            StreamError::CannotUpdateTopicInfo(_) => 7,
            StreamError::CannotOpenTopicInfo(_) => 8,
            StreamError::CannotReadTopicInfo(_) => 9,
            StreamError::CannotCreateTopic(_) => 10,
            StreamError::CannotDeleteTopic(_) => 11,
            StreamError::CannotDeleteTopicDirectory(_) => 12,
            StreamError::CannotPollTopic => 13,
            StreamError::TopicNotFound(_) => 14,
            StreamError::TopicAlreadyExists(_) => 15,
            StreamError::InvalidTopicName => 16,
            StreamError::InvalidTopicPartitions => 17,
            StreamError::LogFileNotFound => 18,
            StreamError::CannotAppendMessage => 19,
            StreamError::CannotCreatePartition => 20,
            StreamError::CannotCreatePartitionDirectory(_, _) => 21,
            StreamError::CannotCreatePartitionSegmentLogFile(_) => 22,
            StreamError::CannotCreatePartitionSegmentIndexFile(_) => 23,
            StreamError::CannotCreatePartitionSegmentTimeIndexFile(_) => 24,
            StreamError::CannotOpenPartitionLogFile => 25,
            StreamError::CannotReadPartitions(_) => 26,
            StreamError::PartitionNotFound(_) => 27,
            StreamError::InvalidMessagesCount => 28,
            StreamError::MessageNotFound => 29,
            StreamError::MessagesNotFound => 30,
            StreamError::SegmentNotFound => 31,
            StreamError::SegmentFull => 32,
            StreamError::CannotReadMessage => 33,
            StreamError::CannotReadMessageTimestamp => 34,
            StreamError::CannotReadMessageLength => 35,
            StreamError::CannotReadMessagePayload => 36
        }
    }
}