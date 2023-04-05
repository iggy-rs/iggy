use std::fmt::{Display, Formatter};

//TODO: Categorize errors in the meaningful way.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum StreamError {
    Error = 1,
    NetworkError = 2,
    InvalidCommand = 3,
    CannotCreateBaseDirectory = 4,
    CannotCreateTopicsDirectory = 11,
    CannotCreateTopicDirectory = 12,
    CannotCreateTopicInfo = 13,
    CannotOpenTopicInfo = 14,
    CannotReadTopicInfo = 15,
    CannotCreateTopic = 20,
    CannotDeleteTopic = 21,
    CannotDeleteTopicDirectory = 22,
    CannotPollTopic = 23,
    TopicNotFound = 24,
    TopicAlreadyExists = 25,
    InvalidTopicName = 26,
    InvalidTopicPartitions = 27,
    LogFileNotFound = 30,
    CannotAppendMessage = 31,
    CannotCreatePartition = 32,
    CannotCreatePartitionDirectory = 33,
    CannotCreatePartitionLogFile = 34,
    CannotCreatePartitionIndexFile = 35,
    CannotCreatePartitionTimeIndexFile = 36,
    CannotOpenPartitionLogFile = 37,
    CannotReadPartitions = 38,
    PartitionNotFound = 39,
    InvalidMessagesCount = 40,
    MessageNotFound = 41,
    MessagesNotFound = 42,
    SegmentNotFound = 43,
    SegmentFull = 44,
    CannotReadMessage = 53,
    CannotReadMessageTimestamp = 54,
    CannotReadMessageLength = 55,
    CannotReadMessagePayload = 56
}

impl Display for StreamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "There was a stream error.")
    }
}