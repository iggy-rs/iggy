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
}

// TODO: Categorize errors in the meaningful way.
impl Error {
    pub fn code(&self) -> u8 {
        match self {
            Error::Error => 0,
            Error::IoError(_) => 1,
            Error::InvalidCommand => 2,
            Error::CannotCreateBaseDirectory => 3,
            Error::CannotCreateStreamsDirectory => 4,
            Error::CannotCreateStreamDirectory(_) => 5,
            Error::CannotCreateStreamInfo(_) => 6,
            Error::CannotUpdateStreamInfo(_) => 7,
            Error::CannotOpenStreamInfo(_) => 8,
            Error::CannotReadStreamInfo(_) => 9,
            Error::CannotCreateStream(_) => 10,
            Error::CannotDeleteStream(_) => 11,
            Error::CannotDeleteStreamDirectory(_) => 12,
            Error::StreamNotFound(_) => 13,
            Error::StreamAlreadyExists(_) => 14,
            Error::InvalidStreamName => 15,
            Error::CannotCreateTopicsDirectory => 16,
            Error::CannotCreateTopicDirectory(_) => 17,
            Error::CannotCreateTopicInfo(_) => 18,
            Error::CannotUpdateTopicInfo(_) => 19,
            Error::CannotOpenTopicInfo(_) => 20,
            Error::CannotReadTopicInfo(_) => 21,
            Error::CannotCreateTopic(_) => 22,
            Error::CannotDeleteTopic(_) => 23,
            Error::CannotDeleteTopicDirectory(_) => 24,
            Error::CannotPollTopic => 25,
            Error::TopicNotFound(_) => 26,
            Error::TopicAlreadyExists(_) => 27,
            Error::InvalidTopicName => 28,
            Error::InvalidTopicPartitions => 29,
            Error::LogFileNotFound => 30,
            Error::CannotAppendMessage => 31,
            Error::CannotCreatePartition => 32,
            Error::CannotCreatePartitionDirectory(_, _) => 33,
            Error::CannotCreatePartitionSegmentLogFile(_) => 34,
            Error::CannotCreatePartitionSegmentIndexFile(_) => 35,
            Error::CannotCreatePartitionSegmentTimeIndexFile(_) => 36,
            Error::CannotOpenPartitionLogFile => 37,
            Error::CannotReadPartitions(_) => 38,
            Error::PartitionNotFound(_) => 39,
            Error::InvalidMessagesCount => 40,
            Error::MessageNotFound => 41,
            Error::MessagesNotFound => 42,
            Error::SegmentNotFound => 43,
            Error::SegmentFull(_, _) => 44,
            Error::InvalidSegmentSize(_) => 42,
            Error::CannotReadMessage => 43,
            Error::CannotReadMessageTimestamp => 42,
            Error::CannotReadMessageLength => 43,
            Error::CannotReadMessagePayload => 44,
            Error::CannotSaveMessagesToSegment => 45,
            Error::CannotSaveIndexToSegment => 46,
            Error::CannotSaveTimeIndexToSegment => 47,
        }
    }
}
