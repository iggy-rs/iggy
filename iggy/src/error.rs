use phf::phf_map;
use quinn::{ConnectionError, ReadError, ReadToEndError, WriteError};
use std::array::TryFromSliceError;
use std::net::AddrParseError;
use std::num::ParseIntError;
use std::str::Utf8Error;
use thiserror::Error;
use tokio::io;

static ERROR_CODES: phf::Map<u32, &'static str> = phf_map! {
            1u32 => "error",
            2u32 => "invalid_configuration",
            3u32 => "invalid_command",
            4u32 => "invalid_format",
            5u32 => "feature_unavailable",
            10u32 => "cannot_create_base_directory",
            20u32 => "resource_not_found",
            21u32 => "cannot_load_resource",
            22u32 => "cannot_save_resource",
            23u32 => "cannot_delete_resource",
            24u32 => "cannot_serialize_resource",
            25u32 => "cannot_deserialize_resource",
            40u32 => "unauthenticated",
            41u32 => "unauthorized",
            42u32 => "invalid_credentials",
            43u32 => "invalid_username",
            44u32 => "invalid_password",
            45u32 => "invalid_user_status",
            46u32 => "user_already_exists",
            47u32 => "user_inactive",
            48u32 => "cannot_delete_user",
            49u32 => "cannot_change_permissions",
            51u32 => "not_connected",
            52u32 => "request_error",
            60u32 => "invalid_encryption_key",
            61u32 => "cannot_encrypt_data",
            62u32 => "cannot_decrypt_data",
            63u32 => "invalid_jwt_encryption_algorithm",
            64u32 => "invalid_jwt_secret",
            100u32 => "client_not_found",
            101u32 => "invalid_client_id",
            200u32 => "io_error",
            201u32 => "write_error",
            202u32 => "cannot_parse_utf8",
            203u32 => "cannot_parse_int",
            204u32 => "cannot_parse_slice",
            300u32 => "http_response_error",
            301u32 => "request_middleware_error",
            302u32 => "cannot_create_endpoint",
            303u32 => "cannot_parse_url",
            304u32 => "invalid_response",
            305u32 => "empty_response",
            306u32 => "cannot_parse_address",
            307u32 => "read_error",
            308u32 => "connection_error",
            309u32 => "read_to_end_error",
            1000u32 => "cannot_create_streams_directory",
            1001u32 => "cannot_create_stream_directory",
            1002u32 => "cannot_create_stream_info",
            1003u32 => "cannot_update_stream_info",
            1004u32 => "cannot_open_stream_info",
            1005u32 => "cannot_read_stream_info",
            1006u32 => "cannot_create_stream",
            1007u32 => "cannot_delete_stream",
            1008u32 => "cannot_delete_stream_directory",
            1009u32 => "stream_id_not_found",
            1010u32 => "stream_name_not_found",
            1011u32 => "stream_id_already_exists",
            1012u32 => "stream_name_already_exists",
            1013u32 => "invalid_stream_name",
            1014u32 => "invalid_stream_id",
            1015u32 => "cannot_read_streams",
            2000u32 => "cannot_create_topics_directory",
            2001u32 => "cannot_create_topic_directory",
            2002u32 => "cannot_create_topic_info",
            2003u32 => "cannot_update_topic_info",
            2004u32 => "cannot_open_topic_info",
            2005u32 => "cannot_read_topic_info",
            2006u32 => "cannot_create_topic",
            2007u32 => "cannot_delete_topic",
            2008u32 => "cannot_delete_topic_directory",
            2009u32 => "cannot_poll_topic",
            2010u32 => "topic_id_not_found",
            2011u32 => "topic_name_not_found",
            2012u32 => "topic_id_already_exists",
            2013u32 => "topic_name_already_exists",
            2014u32 => "invalid_topic_name",
            2015u32 => "too_many_partitions",
            2016u32 => "invalid_topic_id",
            2017u32 => "cannot_read_topics",
            3000u32 => "cannot_create_partition",
            3001u32 => "cannot_create_partitions_directory",
            3002u32 => "cannot_create_partition_directory",
            3003u32 => "cannot_open_partition_log_file",
            3004u32 => "cannot_read_partitions",
            3005u32 => "cannot_delete_partition",
            3006u32 => "cannot_delete_partition_directory",
            3007u32 => "partition_not_found",
            3008u32 => "no_partitions",
            4000u32 => "segment_not_found",
            4001u32 => "segment_closed",
            4002u32 => "invalid_segment_size",
            4003u32 => "cannot_create_segment_log_file",
            4004u32 => "cannot_create_segment_index_file",
            4005u32 => "cannot_create_segment_time_index_file",
            4006u32 => "cannot_save_messages_to_segment",
            4007u32 => "cannot_save_index_to_segment",
            4008u32 => "cannot_save_time_index_to_segment",
            4009u32 => "invalid_messages_count",
            4010u32 => "cannot_append_message",
            4011u32 => "cannot_read_message",
            4012u32 => "cannot_read_message_id",
            4013u32 => "cannot_read_message_state",
            4014u32 => "cannot_read_message_timestamp",
            4015u32 => "cannot_read_headers_length",
            4016u32 => "cannot_read_headers_payload",
            4017u32 => "too_big_headers_payload",
            4018u32 => "invalid_header_key",
            4019u32 => "invalid_header_value",
            4020u32 => "cannot_read_message_length",
            4021u32 => "cannot_read_message_payload",
            4022u32 => "too_big_message_payload",
            4023u32 => "too_many_messages",
            4024u32 => "empty_message_payload",
            4025u32 => "invalid_message_payload_length",
            4026u32 => "cannot_read_message_checksum",
            4027u32 => "invalid_message_checksum",
            4028u32 => "invalid_key_value_length",
            4100u32 => "invalid_offset",
            4101u32 => "cannot_read_consumer_offsets",
            5000u32 => "consumer_group_not_found",
            5001u32 => "consumer_group_already_exists",
            5002u32 => "consumer_group_member_not_found",
            5003u32 => "invalid_consumer_group_id",
            5004u32 => "cannot_create_consumer_groups_directory",
            5005u32 => "cannot_read_consumer_groups",
            5006u32 => "cannot_create_consumer_group_info",
            5007u32 => "cannot_delete_consumer_group_info",
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Empty response")]
    EmptyResponse,
    #[error("Invalid configuration")]
    InvalidConfiguration,
    #[error("Resource with key: {0} was not found.")]
    ResourceNotFound(String),
    #[error("Cannot load resource with key: {0}")]
    CannotLoadResource(String),
    #[error("Cannot save resource with key: {0}")]
    CannotSaveResource(String),
    #[error("Cannot delete resource with key: {0}")]
    CannotDeleteResource(String),
    #[error("Cannot serialize resource with key: {0}")]
    CannotSerializeResource(String),
    #[error("Cannot deserialize resource with key: {0}")]
    CannotDeserializeResource(String),
    #[error("Unauthenticated")]
    Unauthenticated,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Not connected")]
    NotConnected,
    #[error("Request error")]
    RequestError(#[from] reqwest::Error),
    #[error("HTTP response error, status: {0}, body: {1}")]
    HttpResponseError(u16, String),
    #[error("Request middleware error")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),
    #[error("Cannot create endpoint")]
    CannotCreateEndpoint,
    #[error("Cannot parse URL")]
    CannotParseUrl,
    #[error("Invalid response: {0}")]
    InvalidResponse(u32),
    #[error("Cannot parse address")]
    CannotParseAddress(#[from] AddrParseError),
    #[error("Read error")]
    ReadError(#[from] ReadError),
    #[error("Connection error")]
    ConnectionError(#[from] ConnectionError),
    #[error("Read to end error")]
    ReadToEndError(#[from] ReadToEndError),
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
    #[error("Invalid encryption key")]
    InvalidEncryptionKey,
    #[error("Cannot encrypt data")]
    CannotEncryptData,
    #[error("Cannot decrypt data")]
    CannotDecryptData,
    #[error("Invalid JWT algorithm: {0}")]
    InvalidJwtAlgorithm(String),
    #[error("Invalid JWT secret.")]
    InvalidJwtSecret,
    #[error("Invalid command")]
    InvalidCommand,
    #[error("Invalid format")]
    InvalidFormat,
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("Invalid username")]
    InvalidUsername,
    #[error("Invalid password")]
    InvalidPassword,
    #[error("Invalid user status")]
    InvalidUserStatus,
    #[error("User already exists")]
    UserAlreadyExists,
    #[error("User inactive")]
    UserInactive,
    #[error("Cannot delete user with ID: {0}")]
    CannotDeleteUser(u32),
    #[error("Cannot change permissions for user with ID: {0}")]
    CannotChangePermissions(u32),
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
    #[error("Failed to read stream info file for stream with ID: {0}")]
    CannotReadStreamInfo(u32),
    #[error("Cannot read streams")]
    CannotReadStreams,
    #[error("Failed to create stream with ID: {0}")]
    CannotCreateStream(u32),
    #[error("Failed to delete stream with ID: {0}")]
    CannotDeleteStream(u32),
    #[error("Failed to delete stream directory with ID: {0}")]
    CannotDeleteStreamDirectory(u32),
    #[error("Stream with ID: {0} was not found.")]
    StreamIdNotFound(u32),
    #[error("Stream with ID: {0} already exists.")]
    StreamIdAlreadyExists(u32),
    #[error("Stream with name: {0} was not found.")]
    StreamNameNotFound(String),
    #[error("Stream with name: {0} already exists.")]
    StreamNameAlreadyExists(String),
    #[error("Invalid stream ID")]
    InvalidStreamId,
    #[error("Invalid stream name")]
    InvalidStreamName,
    #[error("Cannot read topics for stream with ID: {0}")]
    CannotReadTopics(u32),
    #[error("Cannot create topics directory for stream with ID: {0}")]
    CannotCreateTopicsDirectory(u32),
    #[error("Failed to create directory for topic with ID: {0} for stream with ID: {1}.")]
    CannotCreateTopicDirectory(u32, u32),
    #[error("Failed to create topic info file for topic with ID: {0} for stream with ID: {1}.")]
    CannotCreateTopicInfo(u32, u32),
    #[error("Failed to update topic info for topic with ID: {0} for stream with ID: {1}.")]
    CannotUpdateTopicInfo(u32, u32),
    #[error("Failed to open topic info file for topic with ID: {0} for stream with ID: {1}.")]
    CannotOpenTopicInfo(u32, u32),
    #[error("Failed to read topic info file for topic with ID: {0} for stream with ID: {1}.")]
    CannotReadTopicInfo(u32, u32),
    #[error("Failed to create topic with ID: {0} for stream with ID: {1}.")]
    CannotCreateTopic(u32, u32),
    #[error("Failed to delete topic with ID: {0} for stream with ID: {1}.")]
    CannotDeleteTopic(u32, u32),
    #[error("Failed to delete topic directory with ID: {0} for stream with ID: {1}.")]
    CannotDeleteTopicDirectory(u32, u32),
    #[error("Cannot poll topic")]
    CannotPollTopic,
    #[error("Topic with ID: {0} for stream with ID: {1} was not found.")]
    TopicIdNotFound(u32, u32),
    #[error("Topic with ID: {0} for stream with ID: {1} already exists.")]
    TopicIdAlreadyExists(u32, u32),
    #[error("Topic with name: {0} for stream with ID: {1} was not found.")]
    TopicNameNotFound(String, u32),
    #[error("Topic with name: {0} for stream with ID: {1} already exists.")]
    TopicNameAlreadyExists(String, u32),
    #[error("Invalid topic ID")]
    InvalidTopicId,
    #[error("Invalid topic name")]
    InvalidTopicName,
    #[error("Too many partitions")]
    TooManyPartitions,
    #[error("Cannot append message")]
    CannotAppendMessage,
    #[error("Cannot create partition with ID: {0} for stream with ID: {1} and topic with ID: {2}")]
    CannotCreatePartition(u32, u32, u32),
    #[error("Failed to create directory for partition with ID: {0} for stream with ID: {1} and topic with ID: {2}")]
    CannotCreatePartitionDirectory(u32, u32, u32),
    #[error(
        "Failed to create directory for partitions for stream with ID: {0} and topic with ID: {1}"
    )]
    CannotCreatePartitionsDirectory(u32, u32),
    #[error(
    "Failed to create directory for consumer groups for stream with ID: {0} and topic with ID: {1}"
    )]
    CannotCreateConsumerGroupsDirectory(u32, u32),
    #[error(
        "Failed to delete partition with ID: {0} for stream with ID: {1} and topic with ID: {2}"
    )]
    CannotDeletePartition(u32, u32, u32),
    #[error("Failed to delete partition directory with ID: {0} for stream with ID: {1} and topic with ID: {2}")]
    CannotDeletePartitionDirectory(u32, u32, u32),
    #[error("Failed to create segment log file for path: {0}.")]
    CannotCreateSegmentLogFile(String),
    #[error("Failed to create segment index file for path: {0}.")]
    CannotCreateSegmentIndexFile(String),
    #[error("Failed to create segment time index file for path: {0}.")]
    CannotCreateSegmentTimeIndexFile(String),
    #[error("Cannot open partition log file")]
    CannotOpenPartitionLogFile,
    #[error(
        "Failed to read partitions directories for topic with ID: {0} and stream with ID: {1}"
    )]
    CannotReadPartitions(u32, u32),
    #[error(
        "Partition with ID: {0} for topic with ID: {1} for stream with ID: {2} was not found."
    )]
    PartitionNotFound(u32, u32, u32),
    #[error("Topic with ID: {0} for stream with ID: {1} has no partitions.")]
    NoPartitions(u32, u32),
    #[error("Invalid key value length")]
    InvalidKeyValueLength,
    #[error("Invalid messages count")]
    InvalidMessagesCount,
    #[error("Invalid message payload length")]
    InvalidMessagePayloadLength,
    #[error("Segment not found")]
    SegmentNotFound,
    #[error("Segment with start offset: {0} and partition with ID: {1} is closed")]
    SegmentClosed(u64, u32),
    #[error("Segment size is invalid")]
    InvalidSegmentSize(u64),
    #[error("Cannot read message")]
    CannotReadMessage,
    #[error("Cannot read message timestamp")]
    CannotReadMessageTimestamp,
    #[error("Cannot read message ID")]
    CannotReadMessageId,
    #[error("Cannot read message state")]
    CannotReadMessageState,
    #[error("Cannot read message checksum")]
    CannotReadMessageChecksum,
    #[error("Invalid message checksum: {0}, expected: {1}, for offset: {2}")]
    InvalidMessageChecksum(u32, u32, u64),
    #[error("Cannot read headers length")]
    CannotReadHeadersLength,
    #[error("Cannot read headers payload")]
    CannotReadHeadersPayload,
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
    #[error("Invalid header key")]
    InvalidHeaderKey,
    #[error("Invalid header value")]
    InvalidHeaderValue,
    #[error("Too big headers payload")]
    TooBigHeadersPayload,
    #[error("Invalid offset: {0}")]
    InvalidOffset(u64),
    #[error("Failed to read consumers offsets  for partition with ID: {0}")]
    CannotReadConsumerOffsets(u32),
    #[error("Failed to read consumer groups for topic with ID: {0} and stream with ID: {1}")]
    CannotReadConsumerGroups(u32, u32),
    #[error("Failed to create consumer group info file for ID: {0} for topic with ID: {1} for stream with ID: {2}.")]
    CannotCreateConsumerGroupInfo(u32, u32, u32),
    #[error("Failed to delete consumer group info file for ID: {0} for topic with ID: {1} for stream with ID: {2}.")]
    CannotDeleteConsumerGroupInfo(u32, u32, u32),
    #[error("Consumer group with ID: {0} for topic with ID: {1} was not found.")]
    ConsumerGroupNotFound(u32, u32),
    #[error("Consumer group with ID: {0} for topic with ID: {1} already exists.")]
    ConsumerGroupAlreadyExists(u32, u32),
    #[error("Consumer group member with ID: {0} for group with ID: {1} for topic with ID: {2} was not found.")]
    ConsumerGroupMemberNotFound(u32, u32, u32),
    #[error("Invalid consumer group ID")]
    InvalidConsumerGroupId,
    #[error("Feature is unavailable")]
    FeatureUnavailable,
    #[error("Client with ID: {0} was not found.")]
    ClientNotFound(u32),
    #[error("Invalid client ID")]
    InvalidClientId,
}

impl Error {
    pub fn as_code(&self) -> u32 {
        match self {
            Error::Error => 1,
            Error::InvalidConfiguration => 2,
            Error::InvalidCommand => 3,
            Error::InvalidFormat => 4,
            Error::FeatureUnavailable => 5,
            Error::CannotCreateBaseDirectory => 10,
            Error::ResourceNotFound(_) => 20,
            Error::CannotLoadResource(_) => 21,
            Error::CannotSaveResource(_) => 22,
            Error::CannotDeleteResource(_) => 23,
            Error::CannotSerializeResource(_) => 24,
            Error::CannotDeserializeResource(_) => 25,
            Error::Unauthenticated => 40,
            Error::Unauthorized => 41,
            Error::InvalidCredentials => 42,
            Error::InvalidUsername => 43,
            Error::InvalidPassword => 44,
            Error::InvalidUserStatus => 45,
            Error::UserAlreadyExists => 46,
            Error::UserInactive => 47,
            Error::CannotDeleteUser(_) => 48,
            Error::CannotChangePermissions(_) => 49,
            Error::NotConnected => 51,
            Error::RequestError(_) => 52,
            Error::InvalidEncryptionKey => 60,
            Error::CannotEncryptData => 61,
            Error::CannotDecryptData => 62,
            Error::InvalidJwtAlgorithm(_) => 63,
            Error::InvalidJwtSecret => 64,
            Error::ClientNotFound(_) => 100,
            Error::InvalidClientId => 101,
            Error::IoError(_) => 200,
            Error::WriteError(_) => 201,
            Error::CannotParseUtf8(_) => 202,
            Error::CannotParseInt(_) => 203,
            Error::CannotParseSlice(_) => 204,
            Error::HttpResponseError(_, _) => 300,
            Error::RequestMiddlewareError(_) => 301,
            Error::CannotCreateEndpoint => 302,
            Error::CannotParseUrl => 303,
            Error::InvalidResponse(_) => 304,
            Error::EmptyResponse => 305,
            Error::CannotParseAddress(_) => 306,
            Error::ReadError(_) => 307,
            Error::ConnectionError(_) => 308,
            Error::ReadToEndError(_) => 309,
            Error::CannotCreateStreamsDirectory => 1000,
            Error::CannotCreateStreamDirectory(_) => 1001,
            Error::CannotCreateStreamInfo(_) => 1002,
            Error::CannotUpdateStreamInfo(_) => 1003,
            Error::CannotOpenStreamInfo(_) => 1004,
            Error::CannotReadStreamInfo(_) => 1005,
            Error::CannotCreateStream(_) => 1006,
            Error::CannotDeleteStream(_) => 1007,
            Error::CannotDeleteStreamDirectory(_) => 1008,
            Error::StreamIdNotFound(_) => 1009,
            Error::StreamNameNotFound(_) => 1010,
            Error::StreamIdAlreadyExists(_) => 1011,
            Error::StreamNameAlreadyExists(_) => 1012,
            Error::InvalidStreamName => 1013,
            Error::InvalidStreamId => 1014,
            Error::CannotReadStreams => 1015,
            Error::CannotCreateTopicsDirectory(_) => 2000,
            Error::CannotCreateTopicDirectory(_, _) => 2001,
            Error::CannotCreateTopicInfo(_, _) => 2002,
            Error::CannotUpdateTopicInfo(_, _) => 2003,
            Error::CannotOpenTopicInfo(_, _) => 2004,
            Error::CannotReadTopicInfo(_, _) => 2005,
            Error::CannotCreateTopic(_, _) => 2006,
            Error::CannotDeleteTopic(_, _) => 2007,
            Error::CannotDeleteTopicDirectory(_, _) => 2008,
            Error::CannotPollTopic => 2009,
            Error::TopicIdNotFound(_, _) => 2010,
            Error::TopicNameNotFound(_, _) => 2011,
            Error::TopicIdAlreadyExists(_, _) => 2012,
            Error::TopicNameAlreadyExists(_, _) => 2013,
            Error::InvalidTopicName => 2014,
            Error::TooManyPartitions => 2015,
            Error::InvalidTopicId => 2016,
            Error::CannotReadTopics(_) => 2017,
            Error::CannotCreatePartition(_, _, _) => 3000,
            Error::CannotCreatePartitionsDirectory(_, _) => 3001,
            Error::CannotCreatePartitionDirectory(_, _, _) => 3002,
            Error::CannotOpenPartitionLogFile => 3003,
            Error::CannotReadPartitions(_, _) => 3004,
            Error::CannotDeletePartition(_, _, _) => 3005,
            Error::CannotDeletePartitionDirectory(_, _, _) => 3006,
            Error::PartitionNotFound(_, _, _) => 3007,
            Error::NoPartitions(_, _) => 3008,
            Error::SegmentNotFound => 4000,
            Error::SegmentClosed(_, _) => 4001,
            Error::InvalidSegmentSize(_) => 4002,
            Error::CannotCreateSegmentLogFile(_) => 4003,
            Error::CannotCreateSegmentIndexFile(_) => 4004,
            Error::CannotCreateSegmentTimeIndexFile(_) => 4005,
            Error::CannotSaveMessagesToSegment => 4006,
            Error::CannotSaveIndexToSegment => 4007,
            Error::CannotSaveTimeIndexToSegment => 4008,
            Error::InvalidMessagesCount => 4009,
            Error::CannotAppendMessage => 4010,
            Error::CannotReadMessage => 4011,
            Error::CannotReadMessageId => 4012,
            Error::CannotReadMessageState => 4013,
            Error::CannotReadMessageTimestamp => 4014,
            Error::CannotReadHeadersLength => 4015,
            Error::CannotReadHeadersPayload => 4016,
            Error::TooBigHeadersPayload => 4017,
            Error::InvalidHeaderKey => 4018,
            Error::InvalidHeaderValue => 4019,
            Error::CannotReadMessageLength => 4020,
            Error::CannotReadMessagePayload => 4021,
            Error::TooBigMessagePayload => 4022,
            Error::TooManyMessages => 4023,
            Error::EmptyMessagePayload => 4024,
            Error::InvalidMessagePayloadLength => 4025,
            Error::CannotReadMessageChecksum => 4026,
            Error::InvalidMessageChecksum(_, _, _) => 4027,
            Error::InvalidKeyValueLength => 4028,
            Error::InvalidOffset(_) => 4100,
            Error::CannotReadConsumerOffsets(_) => 4101,
            Error::ConsumerGroupNotFound(_, _) => 5000,
            Error::ConsumerGroupAlreadyExists(_, _) => 5001,
            Error::ConsumerGroupMemberNotFound(_, _, _) => 5002,
            Error::InvalidConsumerGroupId => 5003,
            Error::CannotCreateConsumerGroupsDirectory(_, _) => 5004,
            Error::CannotReadConsumerGroups(_, _) => 5005,
            Error::CannotCreateConsumerGroupInfo(_, _, _) => 5006,
            Error::CannotDeleteConsumerGroupInfo(_, _, _) => 5007,
        }
    }

    pub fn as_str(&self) -> &'static str {
        Self::from_code_as_str(self.as_code())
    }

    pub fn from_code_as_str(code: u32) -> &'static str {
        ERROR_CODES.get(&code).unwrap_or(&"error")
    }
}
