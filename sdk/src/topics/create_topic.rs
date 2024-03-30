use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::topics::{MAX_NAME_LENGTH, MAX_PARTITIONS_COUNT};
use crate::utils::byte_size::IggyByteSize;
use crate::utils::text;
use crate::validatable::Validatable;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::from_utf8;
use crate::compression::compression_algorithm::CompressionAlgorithm;

/// `CreateTopic` command is used to create a new topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric).
/// - `partitions_count` - number of partitions in the topic, max value is 1000.
/// - `message_expiry` - optional message expiry in seconds, if `None` then messages will never expire.
/// - `max_topic_size` - optional maximum size of the topic, if `None` then topic size is unlimited.
///                      Can't be lower than segment size in the config.
/// - `replication_factor` - replication factor for the topic.
/// - `name` - unique topic name, max length is 255 characters. The name will be always converted to lowercase and all whitespaces will be replaced with dots.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateTopic {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric), if None is provided then the server will automatically assign it.
    pub topic_id: Option<u32>,
    /// Number of partitions in the topic, max value is 1000.
    pub partitions_count: u32,
    /// Compression algorithm for the topic.
    pub compression_algorithm: CompressionAlgorithm,
    /// Optional message expiry in seconds, if `None` then messages will never expire.
    pub message_expiry: Option<u32>,
    /// The optional maximum size of the topic.
    pub max_topic_size: Option<IggyByteSize>,
    /// Replication factor for the topic.
    pub replication_factor: u8,
    /// Unique topic name, max length is 255 characters.
    pub name: String,
}

impl CommandPayload for CreateTopic {}

impl Default for CreateTopic {
    fn default() -> Self {
        CreateTopic {
            stream_id: Identifier::default(),
            topic_id: Some(1),
            partitions_count: 1,
            compression_algorithm: CompressionAlgorithm::None,
            message_expiry: None,
            max_topic_size: None,
            replication_factor: 1,
            name: "topic".to_string(),
        }
    }
}

impl Validatable<IggyError> for CreateTopic {
    fn validate(&self) -> Result<(), IggyError> {
        if let Some(topic_id) = self.topic_id {
            if topic_id == 0 {
                return Err(IggyError::InvalidTopicId);
            }
        }

        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(IggyError::InvalidTopicName);
        }

        if !text::is_resource_name_valid(&self.name) {
            return Err(IggyError::InvalidTopicName);
        }

        if !(0..=MAX_PARTITIONS_COUNT).contains(&self.partitions_count) {
            return Err(IggyError::TooManyPartitions);
        }

        if self.replication_factor == 0 {
            return Err(IggyError::InvalidReplicationFactor);
        }

        Ok(())
    }
}

impl BytesSerializable for CreateTopic {
    fn as_bytes(&self) -> Bytes {
        let stream_id_bytes = self.stream_id.as_bytes();
        let mut bytes = BytesMut::with_capacity(23 + stream_id_bytes.len() + self.name.len());
        bytes.put_slice(&stream_id_bytes);
        bytes.put_u32_le(self.topic_id.unwrap_or(0));
        bytes.put_u32_le(self.partitions_count);
        bytes.put_u8(self.compression_algorithm.as_code());
        match self.message_expiry {
            Some(message_expiry) => bytes.put_u32_le(message_expiry),
            None => bytes.put_u32_le(0),
        }
        match self.max_topic_size {
            Some(max_topic_size) => bytes.put_u64_le(max_topic_size.as_bytes_u64()),
            None => bytes.put_u64_le(0),
        }
        bytes.put_u8(self.replication_factor);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.put_slice(self.name.as_bytes());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> std::result::Result<CreateTopic, IggyError> {
        if bytes.len() < 18 {
            return Err(IggyError::InvalidCommand);
        }
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone())?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let topic_id = if topic_id == 0 { None } else { Some(topic_id) };
        let partitions_count = u32::from_le_bytes(bytes[position + 4..position + 8].try_into()?);
        let compression_algorithm = CompressionAlgorithm::from_code(bytes[position + 8])?;
        let message_expiry =
            match u32::from_le_bytes(bytes[position + 9..position + 13].try_into()?) {
                0 => None,
                size => Some(size),
            };
        let max_topic_size =
            match u64::from_le_bytes(bytes[position + 13..position + 21].try_into()?) {
                0 => None,
                size => Some(IggyByteSize::from(size)),
            };
        let replication_factor = bytes[position + 21];
        let name_length = bytes[position + 22];
        let name =
            from_utf8(&bytes[position + 23..(position + 23 + name_length as usize)])?.to_string();
        if name.len() != name_length as usize {
            return Err(IggyError::InvalidCommand);
        }
        let command = CreateTopic {
            stream_id,
            topic_id,
            partitions_count,
            compression_algorithm,
            message_expiry,
            max_topic_size,
            replication_factor,
            name,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for CreateTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let max_topic_size = match self.max_topic_size {
            Some(max_topic_size) => max_topic_size.to_string(),
            None => "unlimited".to_string(),
        };
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{}",
            self.stream_id,
            self.topic_id.unwrap_or(0),
            self.partitions_count,
            self.message_expiry.unwrap_or(0),
            max_topic_size,
            self.replication_factor,
            self.name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreateTopic {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Some(2),
            partitions_count: 3,
            message_expiry: Some(10),
            compression_algorithm: CompressionAlgorithm::None,
            max_topic_size: Some(IggyByteSize::from(100)),
            replication_factor: 1,
            name: "test".to_string(),
        };
        let bytes = command.as_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone()).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let partitions_count =
            u32::from_le_bytes(bytes[position + 4..position + 8].try_into().unwrap());
        let compression_algorithm = CompressionAlgorithm::from_code(bytes[position + 8]).unwrap();
        let message_expiry =
            match u32::from_le_bytes(bytes[position + 9..position + 13].try_into().unwrap()) {
                0 => None,
                secs => Some(secs),
            };
        let max_topic_size =
            match u64::from_le_bytes(bytes[position + 13..position + 21].try_into().unwrap()) {
                0 => None,
                size => Some(IggyByteSize::from(size)),
            };
        let replication_factor = bytes[position + 21];
        let name_length = bytes[position + 22];
        let name = from_utf8(&bytes[position + 23..(position + 23 + name_length as usize)])
            .unwrap()
            .to_string();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id.unwrap());
        assert_eq!(partitions_count, command.partitions_count);
        assert_eq!(compression_algorithm, command.compression_algorithm);
        assert_eq!(message_expiry, command.message_expiry);
        assert_eq!(max_topic_size, command.max_topic_size);
        assert_eq!(replication_factor, command.replication_factor);
        assert_eq!(name.len() as u8, command.name.len() as u8);
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = 2u32;
        let partitions_count = 3u32;
        let compression_algorithm = CompressionAlgorithm::None;
        let name = "test".to_string();
        let message_expiry = 10;
        let max_topic_size = IggyByteSize::from(100);
        let replication_factor = 1;
        let stream_id_bytes = stream_id.as_bytes();
        let mut bytes = BytesMut::with_capacity(14 + stream_id_bytes.len() + name.len());
        bytes.put_slice(&stream_id_bytes);
        bytes.put_u32_le(topic_id);
        bytes.put_u32_le(partitions_count);
        bytes.put_u8(compression_algorithm.as_code());
        bytes.put_u32_le(message_expiry);
        bytes.put_u64_le(max_topic_size.as_bytes_u64());
        bytes.put_u8(replication_factor);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.put_slice(name.as_bytes());

        let command = CreateTopic::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id.unwrap(), topic_id);
        assert_eq!(command.name, name);
        assert_eq!(command.partitions_count, partitions_count);
        assert_eq!(command.compression_algorithm, compression_algorithm);
        assert_eq!(command.message_expiry, Some(message_expiry));
        assert_eq!(command.max_topic_size, Some(max_topic_size));
        assert_eq!(command.replication_factor, replication_factor);
        assert_eq!(command.partitions_count, partitions_count);
    }
}
