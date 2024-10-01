use crate::utils::byte_size::IggyByteSize;
use crate::utils::topic_size::MaxTopicSize;
use strum::{EnumDiscriminants, FromRepr, IntoStaticStr};
use thiserror::Error;

#[derive(Debug, Error, EnumDiscriminants, IntoStaticStr)]
#[repr(u32)]
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(
    vis(pub(crate)),
    derive(FromRepr, IntoStaticStr),
    strum(serialize_all = "snake_case")
)]
pub enum IggyError {
    #[error("Error")]
    Error = 1,
    #[error("Invalid configuration")]
    InvalidConfiguration = 2,
    #[error("Invalid command")]
    InvalidCommand = 3,
    #[error("Invalid format")]
    InvalidFormat = 4,
    #[error("Feature is unavailable")]
    FeatureUnavailable = 5,
    #[error("Invalid identifier")]
    InvalidIdentifier = 6,
    #[error("Invalid version: {0}")]
    InvalidVersion(String) = 7,
    #[error("Disconnected")]
    Disconnected = 8,
    #[error("Cannot establish connection")]
    CannotEstablishConnection = 9,
    #[error("Cannot create base directory, Path: {0}")]
    CannotCreateBaseDirectory(String) = 10,
    #[error("Cannot create runtime directory, Path: {0}")]
    CannotCreateRuntimeDirectory(String) = 11,
    #[error("Cannot remove runtime directory, Path: {0}")]
    CannotRemoveRuntimeDirectory(String) = 12,
    #[error("Cannot create state directory, Path: {0}")]
    CannotCreateStateDirectory(String) = 13,
    #[error("State file not found")]
    StateFileNotFound = 14,
    #[error("State file corrupted")]
    StateFileCorrupted = 15,
    #[error("Invalid state entry checksum: {0}, expected: {1}, for index: {2}")]
    InvalidStateEntryChecksum(u32, u32, u64) = 16,
    #[error("Cannot open database, Path: {0}")]
    CannotOpenDatabase(String) = 19,
    #[error("Resource with key: {0} was not found.")]
    ResourceNotFound(String) = 20,
    #[error("Cannot load resource. Reason: {0:#}")]
    CannotLoadResource(#[source] anyhow::Error) = 21,
    #[error("Cannot save resource. Reason: {0:#}")]
    CannotSaveResource(#[source] anyhow::Error) = 22,
    #[error("Cannot delete resource. Reason: {0:#}")]
    CannotDeleteResource(#[source] anyhow::Error) = 23,
    #[error("Cannot serialize resource. Reason: {0:#}")]
    CannotSerializeResource(#[source] anyhow::Error) = 24,
    #[error("Cannot deserialize resource. Reason: {0:#}")]
    CannotDeserializeResource(#[source] anyhow::Error) = 25,
    #[error("Stale client")]
    StaleClient = 30,
    #[error("Unauthenticated")]
    Unauthenticated = 40,
    #[error("Unauthorized")]
    Unauthorized = 41,
    #[error("Invalid credentials")]
    InvalidCredentials = 42,
    #[error("Invalid username")]
    InvalidUsername = 43,
    #[error("Invalid password")]
    InvalidPassword = 44,
    #[error("Invalid user status")]
    InvalidUserStatus = 45,
    #[error("User already exists")]
    UserAlreadyExists = 46,
    #[error("User inactive")]
    UserInactive = 47,
    #[error("Cannot delete user with ID: {0}")]
    CannotDeleteUser(u32) = 48,
    #[error("Cannot change permissions for user with ID: {0}")]
    CannotChangePermissions(u32) = 49,
    #[error("Invalid personal access token name")]
    InvalidPersonalAccessTokenName = 50,
    #[error("Personal access token: {0} for user with ID: {1} already exists")]
    PersonalAccessTokenAlreadyExists(String, u32) = 51,
    #[error("User with ID: {0} has reached the maximum number of personal access tokens: {1}")]
    PersonalAccessTokensLimitReached(u32, u32) = 52,
    #[error("Invalid personal access token")]
    InvalidPersonalAccessToken = 53,
    #[error("Personal access token: {0} for user with ID: {1} has expired.")]
    PersonalAccessTokenExpired(String, u32) = 54,
    #[error("Users limit reached.")]
    UsersLimitReached = 55,
    #[error("Not connected")]
    NotConnected = 61,
    #[error("Request error")]
    RequestError(#[from] reqwest::Error) = 62,
    #[error("Client shutdown")]
    ClientShutdown = 63,
    #[error("Invalid encryption key")]
    InvalidEncryptionKey = 70,
    #[error("Cannot encrypt data")]
    CannotEncryptData = 71,
    #[error("Cannot decrypt data")]
    CannotDecryptData = 72,
    #[error("Invalid JWT algorithm: {0}")]
    InvalidJwtAlgorithm(String) = 73,
    #[error("Invalid JWT secret")]
    InvalidJwtSecret = 74,
    #[error("JWT is missing")]
    JwtMissing = 75,
    #[error("Cannot generate JWT")]
    CannotGenerateJwt = 76,
    #[error("Access token is missing")]
    AccessTokenMissing = 77,
    #[error("Invalid access token")]
    InvalidAccessToken = 78,
    #[error("Client with ID: {0} was not found.")]
    ClientNotFound(u32) = 100,
    #[error("Invalid client ID")]
    InvalidClientId = 101,
    #[error("IO error")]
    IoError(#[from] std::io::Error) = 200,
    #[error("Write error")]
    WriteError(#[from] quinn::WriteError) = 201,
    #[error("Cannot parse UTF8")]
    CannotParseUtf8(#[from] std::str::Utf8Error) = 202,
    #[error("Cannot parse integer")]
    CannotParseInt(#[from] std::num::ParseIntError) = 203,
    #[error("Cannot parse integer")]
    CannotParseSlice(#[from] std::array::TryFromSliceError) = 204,
    #[error("Cannot parse byte unit")]
    CannotParseByteUnit(#[from] byte_unit::ParseError) = 205,
    #[error("Connection closed")]
    ConnectionClosed = 206,
    #[error("Cannot parse float")]
    CannotParseFloat(#[from] std::num::ParseFloatError) = 207,
    #[error("Cannot parse bool")]
    CannotParseBool(#[from] std::str::ParseBoolError) = 208,
    #[error("Cannot parse header kind from {0}")]
    CannotParseHeaderKind(String) = 209,
    #[error("HTTP response error, status: {0}, body: {1}")]
    HttpResponseError(u16, String) = 300,
    #[error("Request middleware error")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error) = 301,
    #[error("Cannot create endpoint")]
    CannotCreateEndpoint = 302,
    #[error("Cannot parse URL")]
    CannotParseUrl = 303,
    #[error("Invalid response: {0}: {2})")]
    InvalidResponse(u32, u32, String) = 304,
    #[error("Empty response")]
    EmptyResponse = 305,
    #[error("Cannot parse address")]
    CannotParseAddress(#[from] std::net::AddrParseError) = 306,
    #[error("Read error")]
    ReadError(#[from] quinn::ReadError) = 307,
    #[error("Connection error")]
    ConnectionError(#[from] quinn::ConnectionError) = 308,
    #[error("Read to end error")]
    ReadToEndError(#[from] quinn::ReadToEndError) = 309,
    #[error("Closed error")]
    ClosedError(#[from] quinn::ClosedStream) = 310,
    #[error("Cannot create streams directory, Path: {0}")]
    CannotCreateStreamsDirectory(String) = 1000,
    #[error("Cannot create stream with ID: {0} directory, Path: {1}")]
    CannotCreateStreamDirectory(u32, String) = 1001,
    #[error("Failed to create stream info file for stream with ID: {0}")]
    CannotCreateStreamInfo(u32) = 1002,
    #[error("Failed to update stream info for stream with ID: {0}")]
    CannotUpdateStreamInfo(u32) = 1003,
    #[error("Failed to open stream info file for stream with ID: {0}")]
    CannotOpenStreamInfo(u32) = 1004,
    #[error("Failed to read stream info file for stream with ID: {0}")]
    CannotReadStreamInfo(u32) = 1005,
    #[error("Failed to create stream with ID: {0}")]
    CannotCreateStream(u32) = 1006,
    #[error("Failed to delete stream with ID: {0}")]
    CannotDeleteStream(u32) = 1007,
    #[error("Failed to delete stream directory with ID: {0}")]
    CannotDeleteStreamDirectory(u32) = 1008,
    #[error("Stream with ID: {0} was not found.")]
    StreamIdNotFound(u32) = 1009,
    #[error("Stream with name: {0} was not found.")]
    StreamNameNotFound(String) = 1010,
    #[error("Stream with ID: {0} already exists.")]
    StreamIdAlreadyExists(u32) = 1011,
    #[error("Stream with name: {0} already exists.")]
    StreamNameAlreadyExists(String) = 1012,
    #[error("Invalid stream name")]
    InvalidStreamName = 1013,
    #[error("Invalid stream ID")]
    InvalidStreamId = 1014,
    #[error("Cannot read streams")]
    CannotReadStreams = 1015,
    #[error("Missing streams")]
    MissingStreams = 1016,
    #[error("Missing topics for stream with ID: {0}")]
    MissingTopics(u32) = 1017,
    #[error("Missing partitions for topic with ID: {0} for stream with ID: {1}.")]
    MissingPartitions(u32, u32) = 1018,
    #[error("Max topic size cannot be lower than segment size. Max topic size: {0} < segment size: {1}.")]
    InvalidTopicSize(MaxTopicSize, IggyByteSize) = 1019,
    #[error("Cannot create topics directory for stream with ID: {0}, Path: {1}")]
    CannotCreateTopicsDirectory(u32, String) = 2000,
    #[error(
        "Failed to create directory for topic with ID: {0} for stream with ID: {1}, Path: {2}"
    )]
    CannotCreateTopicDirectory(u32, u32, String) = 2001,
    #[error("Failed to create topic info file for topic with ID: {0} for stream with ID: {1}.")]
    CannotCreateTopicInfo(u32, u32) = 2002,
    #[error("Failed to update topic info for topic with ID: {0} for stream with ID: {1}.")]
    CannotUpdateTopicInfo(u32, u32) = 2003,
    #[error("Failed to open topic info file for topic with ID: {0} for stream with ID: {1}.")]
    CannotOpenTopicInfo(u32, u32) = 2004,
    #[error("Failed to read topic info file for topic with ID: {0} for stream with ID: {1}.")]
    CannotReadTopicInfo(u32, u32) = 2005,
    #[error("Failed to create topic with ID: {0} for stream with ID: {1}.")]
    CannotCreateTopic(u32, u32) = 2006,
    #[error("Failed to delete topic with ID: {0} for stream with ID: {1}.")]
    CannotDeleteTopic(u32, u32) = 2007,
    #[error("Failed to delete topic directory with ID: {0} for stream with ID: {1}, Path: {2}")]
    CannotDeleteTopicDirectory(u32, u32, String) = 2008,
    #[error("Cannot poll topic")]
    CannotPollTopic = 2009,
    #[error("Topic with ID: {0} for stream with ID: {1} was not found.")]
    TopicIdNotFound(u32, u32) = 2010,
    #[error("Topic with name: {0} for stream with ID: {1} was not found.")]
    TopicNameNotFound(String, String) = 2011,
    #[error("Topic with ID: {0} for stream with ID: {1} already exists.")]
    TopicIdAlreadyExists(u32, u32) = 2012,
    #[error("Topic with name: {0} for stream with ID: {1} already exists.")]
    TopicNameAlreadyExists(String, u32) = 2013,
    #[error("Invalid topic name")]
    InvalidTopicName = 2014,
    #[error("Too many partitions")]
    TooManyPartitions = 2015,
    #[error("Invalid topic ID")]
    InvalidTopicId = 2016,
    #[error("Cannot read topics for stream with ID: {0}")]
    CannotReadTopics(u32) = 2017,
    #[error("Invalid replication factor")]
    InvalidReplicationFactor = 2018,
    #[error("Cannot create partition with ID: {0} for stream with ID: {1} and topic with ID: {2}")]
    CannotCreatePartition(u32, u32, u32) = 3000,
    #[error(
        "Failed to create directory for partitions for stream with ID: {0} and topic with ID: {1}"
    )]
    CannotCreatePartitionsDirectory(u32, u32) = 3001,
    #[error("Failed to create directory for partition with ID: {0} for stream with ID: {1} and topic with ID: {2}")]
    CannotCreatePartitionDirectory(u32, u32, u32) = 3002,
    #[error("Cannot open partition log file")]
    CannotOpenPartitionLogFile = 3003,
    #[error("Cannot read partitions directories. Reason: {0:#}")]
    CannotReadPartitions(#[source] anyhow::Error) = 3004,
    #[error(
        "Failed to delete partition with ID: {0} for stream with ID: {1} and topic with ID: {2}"
    )]
    CannotDeletePartition(u32, u32, u32) = 3005,
    #[error("Failed to delete partition directory with ID: {0} for stream with ID: {1} and topic with ID: {2}")]
    CannotDeletePartitionDirectory(u32, u32, u32) = 3006,
    #[error(
        "Partition with ID: {0} for topic with ID: {1} for stream with ID: {2} was not found."
    )]
    PartitionNotFound(u32, u32, u32) = 3007,
    #[error("Topic with ID: {0} for stream with ID: {1} has no partitions.")]
    NoPartitions(u32, u32) = 3008,
    #[error("Cannot read partitions for topic with ID: {0} for stream with ID: {1}")]
    TopicFull(u32, u32) = 3009,
    #[error("Failed to delete consumer offsets directory for path: {0}")]
    CannotDeleteConsumerOffsetsDirectory(String) = 3010,
    #[error("Failed to delete consumer offset file for path: {0}")]
    CannotDeleteConsumerOffsetFile(String) = 3011,
    #[error("Failed to create consumer offsets directory for path: {0}")]
    CannotCreateConsumerOffsetsDirectory(String) = 3012,
    #[error("Failed to read consumers offsets from path: {0}")]
    CannotReadConsumerOffsets(String) = 3020,
    #[error("Segment not found")]
    SegmentNotFound = 4000,
    #[error("Segment with start offset: {0} and partition with ID: {1} is closed")]
    SegmentClosed(u64, u32) = 4001,
    #[error("Segment size is invalid")]
    InvalidSegmentSize(u64) = 4002,
    #[error("Failed to create segment log file for Path: {0}.")]
    CannotCreateSegmentLogFile(String) = 4003,
    #[error("Failed to create segment index file for Path: {0}.")]
    CannotCreateSegmentIndexFile(String) = 4004,
    #[error("Failed to create segment time index file for Path: {0}.")]
    CannotCreateSegmentTimeIndexFile(String) = 4005,
    #[error("Cannot save messages to segment. Reason: {0:#}")]
    CannotSaveMessagesToSegment(#[source] anyhow::Error) = 4006,
    #[error("Cannot save index to segment. Reason: {0:#}")]
    CannotSaveIndexToSegment(#[source] anyhow::Error) = 4007,
    #[error("Cannot save time index to segment. Reason: {0:#}")]
    CannotSaveTimeIndexToSegment(#[source] anyhow::Error) = 4008,
    #[error("Invalid messages count")]
    InvalidMessagesCount = 4009,
    #[error("Cannot append message")]
    CannotAppendMessage = 4010,
    #[error("Cannot read message")]
    CannotReadMessage = 4011,
    #[error("Cannot read message ID")]
    CannotReadMessageId = 4012,
    #[error("Cannot read message state")]
    CannotReadMessageState = 4013,
    #[error("Cannot read message timestamp")]
    CannotReadMessageTimestamp = 4014,
    #[error("Cannot read headers length")]
    CannotReadHeadersLength = 4015,
    #[error("Cannot read headers payload")]
    CannotReadHeadersPayload = 4016,
    #[error("Too big headers payload")]
    TooBigHeadersPayload = 4017,
    #[error("Invalid header key")]
    InvalidHeaderKey = 4018,
    #[error("Invalid header value")]
    InvalidHeaderValue = 4019,
    #[error("Cannot read message length")]
    CannotReadMessageLength = 4020,
    #[error("Cannot save messages to segment")]
    CannotReadMessagePayload = 4021,
    #[error("Too big message payload")]
    TooBigMessagePayload = 4022,
    #[error("Too many messages")]
    TooManyMessages = 4023,
    #[error("Empty message payload")]
    EmptyMessagePayload = 4024,
    #[error("Invalid message payload length")]
    InvalidMessagePayloadLength = 4025,
    #[error("Cannot read message checksum")]
    CannotReadMessageChecksum = 4026,
    #[error("Invalid message checksum: {0}, expected: {1}, for offset: {2}")]
    InvalidMessageChecksum(u32, u32, u64) = 4027,
    #[error("Invalid key value length")]
    InvalidKeyValueLength = 4028,
    #[error("Command length error: {0}")]
    CommandLengthError(String) = 4029,
    #[error("Invalid offset: {0}")]
    InvalidOffset(u64) = 4100,
    #[error("Consumer group with ID: {0} for topic with ID: {1} was not found.")]
    ConsumerGroupIdNotFound(u32, u32) = 5000,
    #[error("Consumer group with ID: {0} for topic with ID: {1} already exists.")]
    ConsumerGroupIdAlreadyExists(u32, u32) = 5001,
    #[error("Invalid consumer group ID")]
    InvalidConsumerGroupId = 5002,
    #[error("Consumer group with name: {0} for topic with ID: {1} was not found.")]
    ConsumerGroupNameNotFound(String, String) = 5003,
    #[error("Consumer group with name: {0} for topic with ID: {1} already exists.")]
    ConsumerGroupNameAlreadyExists(String, u32) = 5004,
    #[error("Invalid consumer group name")]
    InvalidConsumerGroupName = 5005,
    #[error("Consumer group member with ID: {0} for group with ID: {1} for topic with ID: {2} was not found.")]
    ConsumerGroupMemberNotFound(u32, u32, u32) = 5006,
    #[error("Failed to create consumer group info file for ID: {0} for topic with ID: {1} for stream with ID: {2}.")]
    CannotCreateConsumerGroupInfo(u32, u32, u32) = 5007,
    #[error("Failed to delete consumer group info file for ID: {0} for topic with ID: {1} for stream with ID: {2}.")]
    CannotDeleteConsumerGroupInfo(u32, u32, u32) = 5008,
    #[error("Base offset is missing")]
    MissingBaseOffsetRetainedMessageBatch = 6000,
    #[error("Last offset delta is missing")]
    MissingLastOffsetDeltaRetainedMessageBatch = 6001,
    #[error("Max timestamp is missing")]
    MissingMaxTimestampRetainedMessageBatch = 6002,
    #[error("Length is missing")]
    MissingLengthRetainedMessageBatch = 6003,
    #[error("Payload is missing")]
    MissingPayloadRetainedMessageBatch = 6004,
    #[error("Cannot read batch base offset")]
    CannotReadBatchBaseOffset = 7000,
    #[error("Cannot read batch length")]
    CannotReadBatchLength = 7001,
    #[error("Cannot read batch last offset delta")]
    CannotReadLastOffsetDelta = 7002,
    #[error("Cannot read batch maximum timestamp")]
    CannotReadMaxTimestamp = 7003,
    #[error("Cannot read batch payload")]
    CannotReadBatchPayload = 7004,
    #[error("Invalid connection string")]
    InvalidConnectionString = 8000,
}

impl IggyError {
    pub fn as_code(&self) -> u32 {
        // SAFETY: SdkError specifies #[repr(u32)] representation.
        // https://doc.rust-lang.org/reference/items/enumerations.html#pointer-casting
        unsafe { *(self as *const Self as *const u32) }
    }

    pub fn as_string(&self) -> &'static str {
        self.into()
    }

    pub fn from_code_as_string(code: u32) -> &'static str {
        IggyErrorDiscriminants::from_repr(code)
            .map(|discriminant| discriminant.into())
            .unwrap_or("unknown error code")
    }
}

impl PartialEq for IggyError {
    fn eq(&self, other: &Self) -> bool {
        self.as_code() == other.as_code()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GROUP_NAME_ERROR_CODE: u32 = 5005;

    #[test]
    fn derived_sdk_error_discriminant_keeps_codes() {
        assert_eq!(
            GROUP_NAME_ERROR_CODE,
            IggyError::InvalidConsumerGroupName.as_code()
        );
        assert_eq!(
            GROUP_NAME_ERROR_CODE,
            IggyErrorDiscriminants::InvalidConsumerGroupName as u32
        );
    }

    #[test]
    fn static_str_uses_kebab_case() {
        assert_eq!(
            "invalid_consumer_group_name",
            IggyError::InvalidConsumerGroupName.as_string()
        )
    }

    #[test]
    fn gets_string_from_code() {
        assert_eq!(
            IggyError::InvalidConsumerGroupName.as_string(),
            IggyError::from_code_as_string(GROUP_NAME_ERROR_CODE)
        )
    }
}
