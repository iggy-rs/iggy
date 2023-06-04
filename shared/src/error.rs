use quinn::WriteError;
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
    #[error("Write error")]
    WriteError(#[from] WriteError),
    #[error("Cannot parse integer")]
    CannotParseInt(#[from] ParseIntError),
    #[error("Cannot parse integer")]
    CannotParseSlice(#[from] TryFromSliceError),
    #[error("Cannot parse UTF8")]
    CannotParseUtf8(#[from] Utf8Error),
    #[error("Invalid command")]
    InvalidCommand,
    #[error("Invalid format")]
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
    #[error("Invalid stream ID")]
    InvalidStreamId,
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
    #[error("Invalid topic ID")]
    InvalidTopicId,
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
    #[error("Failed to create directory for partition with ID: {0}")]
    CannotCreatePartitionDirectory(u32),
    #[error("Failed to delete partition with ID: {0}")]
    CannotDeletePartition(u32),
    #[error("Failed to delete partition directory with ID: {0}")]
    CannotDeletePartitionDirectory(u32),
    #[error("Failed to create partition segment log file for path: {0}.")]
    CannotCreatePartitionSegmentLogFile(String),
    #[error("Failed to create partition segment index file for path: {0}.")]
    CannotCreatePartitionSegmentIndexFile(String),
    #[error("Failed to create partition segment time index file for path: {0}.")]
    CannotCreatePartitionSegmentTimeIndexFile(String),
    #[error("Cannot open partition log file")]
    CannotOpenPartitionLogFile,
    #[error("Failed to partitions directories for topic with ID: {0}")]
    CannotReadPartitions(u32),
    #[error("Partition with ID: {0} was not found.")]
    PartitionNotFound(u32),
    #[error("Invalid messages count")]
    InvalidMessagesCount,
    #[error("Invalid message payload length")]
    InvalidMessagePayloadLength,
    #[error("Segment not found")]
    SegmentNotFound,
    #[error("Segment with start offset: {0} and partition ID: {1} is closed")]
    SegmentClosed(u64, u32),
    #[error("Segment size is invalid")]
    InvalidSegmentSize(u64),
    #[error("Cannot read message")]
    CannotReadMessage,
    #[error("Cannot read message timestamp")]
    CannotReadMessageTimestamp,
    #[error("Cannot read message ID")]
    CannotReadMessageId,
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
    #[error("Empty message payload")]
    EmptyMessagePayload,
    #[error("Too big message payload")]
    TooBigMessagePayload,
    #[error("Too many messages")]
    TooManyMessages,
    #[error("Invalid offset: {0}")]
    InvalidOffset(u64),
    #[error("Failed to read consumers offsets  for partition with ID: {0}")]
    CannotReadConsumerOffsets(u32),
}

impl Error {
    pub fn as_code(&self) -> u8 {
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
            Error::CannotCreatePartitionDirectory(_) => 34,
            Error::CannotCreatePartitionSegmentLogFile(_) => 35,
            Error::CannotCreatePartitionSegmentIndexFile(_) => 36,
            Error::CannotCreatePartitionSegmentTimeIndexFile(_) => 37,
            Error::CannotOpenPartitionLogFile => 38,
            Error::CannotReadPartitions(_) => 39,
            Error::PartitionNotFound(_) => 40,
            Error::InvalidMessagesCount => 41,
            Error::InvalidStreamId => 42,
            Error::InvalidTopicId => 43,
            Error::SegmentNotFound => 44,
            Error::SegmentClosed(_, _) => 45,
            Error::InvalidSegmentSize(_) => 46,
            Error::CannotReadMessage => 47,
            Error::CannotReadMessageTimestamp => 48,
            Error::CannotReadMessageId => 49,
            Error::CannotReadMessageLength => 50,
            Error::CannotReadMessagePayload => 51,
            Error::CannotSaveMessagesToSegment => 52,
            Error::CannotSaveIndexToSegment => 53,
            Error::CannotSaveTimeIndexToSegment => 54,
            Error::CannotParseUtf8(_) => 55,
            Error::CannotParseInt(_) => 56,
            Error::CannotParseSlice(_) => 57,
            Error::TooBigMessagePayload => 58,
            Error::TooManyMessages => 59,
            Error::WriteError(_) => 60,
            Error::InvalidOffset(_) => 61,
            Error::CannotReadConsumerOffsets(_) => 62,
            Error::CannotDeletePartition(_) => 63,
            Error::CannotDeletePartitionDirectory(_) => 64,
            Error::InvalidMessagePayloadLength => 65,
            Error::EmptyMessagePayload => 67,
        }
    }

    pub fn as_text_code(&self) -> &'static str {
        match self {
            Error::Error => "error",
            Error::IoError(_) => "io_error",
            Error::InvalidCommand => "invalid_command",
            Error::InvalidFormat => "invalid_format",
            Error::CannotCreateBaseDirectory => "cannot_create_base_directory",
            Error::CannotCreateStreamsDirectory => "cannot_create_streams_directory",
            Error::CannotCreateStreamDirectory(_) => "cannot_create_stream_directory",
            Error::CannotCreateStreamInfo(_) => "cannot_create_stream_info",
            Error::CannotUpdateStreamInfo(_) => "cannot_update_stream_info",
            Error::CannotOpenStreamInfo(_) => "cannot_open_stream_info",
            Error::CannotReadStreamInfo(_) => "cannot_read_stream_info",
            Error::CannotCreateStream(_) => "cannot_create_stream",
            Error::CannotDeleteStream(_) => "cannot_delete_stream",
            Error::CannotDeleteStreamDirectory(_) => "cannot_delete_stream_directory",
            Error::StreamNotFound(_) => "stream_not_found",
            Error::StreamAlreadyExists(_) => "stream_already_exists",
            Error::InvalidStreamName => "invalid_stream_name",
            Error::CannotCreateTopicsDirectory => "cannot_create_topics_directory",
            Error::CannotCreateTopicDirectory(_) => "cannot_create_topic_directory",
            Error::CannotCreateTopicInfo(_) => "cannot_create_topic_info",
            Error::CannotUpdateTopicInfo(_) => "cannot_update_topic_info",
            Error::CannotOpenTopicInfo(_) => "cannot_open_topic_info",
            Error::CannotReadTopicInfo(_) => "cannot_read_topic_info",
            Error::CannotCreateTopic(_) => "cannot_create_topic",
            Error::CannotDeleteTopic(_) => "cannot_delete_topic",
            Error::CannotDeleteTopicDirectory(_) => "cannot_delete_topic_directory",
            Error::CannotPollTopic => "cannot_poll_topic",
            Error::TopicNotFound(_) => "topic_not_found",
            Error::TopicAlreadyExists(_) => "topic_already_exists",
            Error::InvalidTopicName => "invalid_topic_name",
            Error::InvalidTopicPartitions => "invalid_topic_partitions",
            Error::LogFileNotFound => "log_file_not_found",
            Error::CannotAppendMessage => "cannot_append_message",
            Error::CannotCreatePartition => "cannot_create_partition",
            Error::CannotCreatePartitionDirectory(_) => "cannot_create_partition_directory",
            Error::CannotCreatePartitionSegmentLogFile(_) => {
                "cannot_create_partition_segment_log_file"
            }
            Error::CannotCreatePartitionSegmentIndexFile(_) => {
                "cannot_create_partition_segment_index_file"
            }
            Error::CannotCreatePartitionSegmentTimeIndexFile(_) => {
                "cannot_create_partition_segment_time_index_file"
            }
            Error::CannotOpenPartitionLogFile => "cannot_open_partition_log_file",
            Error::CannotReadPartitions(_) => "cannot_read_partitions",
            Error::PartitionNotFound(_) => "partition_not_found",
            Error::InvalidMessagesCount => "invalid_messages_count",
            Error::InvalidStreamId => "invalid_stream_id",
            Error::InvalidTopicId => "invalid_topic_id",
            Error::SegmentNotFound => "segment_not_found",
            Error::SegmentClosed(_, _) => "segment_closed",
            Error::InvalidSegmentSize(_) => "invalid_segment_size",
            Error::CannotReadMessage => "cannot_read_message",
            Error::CannotReadMessageTimestamp => "cannot_read_message_timestamp",
            Error::CannotReadMessageId => "cannot_read_message_id",
            Error::CannotReadMessageLength => "cannot_read_message_length",
            Error::CannotReadMessagePayload => "cannot_read_message_payload",
            Error::CannotSaveMessagesToSegment => "cannot_save_messages_to_segment",
            Error::CannotSaveIndexToSegment => "cannot_save_index_to_segment",
            Error::CannotSaveTimeIndexToSegment => "cannot_save_time_index_to_segment",
            Error::CannotParseUtf8(_) => "cannot_parse_utf8",
            Error::CannotParseInt(_) => "cannot_parse_int",
            Error::CannotParseSlice(_) => "cannot_parse_slice",
            Error::TooBigMessagePayload => "too_big_payload",
            Error::TooManyMessages => "too_many_messages",
            Error::WriteError(_) => "write_error",
            Error::InvalidOffset(_) => "invalid_offset",
            Error::CannotReadConsumerOffsets(_) => "cannot_read_consumer_offsets",
            Error::CannotDeletePartition(_) => "cannot_delete_partition",
            Error::CannotDeletePartitionDirectory(_) => "cannot_delete_partition_directory",
            Error::InvalidMessagePayloadLength => "invalid_message_payload_length",
            Error::EmptyMessagePayload => "empty_message_payload",
        }
    }
}
