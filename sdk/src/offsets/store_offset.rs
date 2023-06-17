use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct StoreOffset {
    #[serde(default = "default_consumer_id")]
    pub consumer_id: u32,
    #[serde(skip)]
    pub stream_id: u32,
    #[serde(skip)]
    pub topic_id: u32,
    pub partition_id: u32,
    pub offset: u64,
}

impl Default for StoreOffset {
    fn default() -> Self {
        StoreOffset {
            consumer_id: default_consumer_id(),
            stream_id: 1,
            topic_id: 1,
            partition_id: 1,
            offset: 0,
        }
    }
}

impl CommandPayload for StoreOffset {}

fn default_consumer_id() -> u32 {
    0
}

impl Validatable for StoreOffset {
    fn validate(&self) -> Result<(), Error> {
        if self.stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        if self.topic_id == 0 {
            return Err(Error::InvalidTopicId);
        }

        Ok(())
    }
}

impl FromStr for StoreOffset {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 5 {
            return Err(Error::InvalidCommand);
        }

        let consumer_id = parts[0].parse::<u32>()?;
        let stream_id = parts[1].parse::<u32>()?;
        let topic_id = parts[2].parse::<u32>()?;
        let partition_id = parts[3].parse::<u32>()?;
        let offset = parts[4].parse::<u64>()?;
        let command = StoreOffset {
            consumer_id,
            stream_id,
            topic_id,
            partition_id,
            offset,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for StoreOffset {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(24);
        bytes.extend(self.consumer_id.to_le_bytes());
        bytes.extend(self.stream_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        bytes.extend(self.partition_id.to_le_bytes());
        bytes.extend(self.offset.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<StoreOffset, Error> {
        if bytes.len() != 24 {
            return Err(Error::InvalidCommand);
        }

        let consumer_id = u32::from_le_bytes(bytes[..4].try_into()?);
        let stream_id = u32::from_le_bytes(bytes[4..8].try_into()?);
        let topic_id = u32::from_le_bytes(bytes[8..12].try_into()?);
        let partition_id = u32::from_le_bytes(bytes[12..16].try_into()?);
        let offset = u64::from_le_bytes(bytes[16..24].try_into()?);
        let command = StoreOffset {
            consumer_id,
            stream_id,
            topic_id,
            partition_id,
            offset,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for StoreOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}",
            self.consumer_id, self.stream_id, self.topic_id, self.partition_id, self.offset
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let is_empty = false;
        let command = StoreOffset {
            consumer_id: 1,
            stream_id: 2,
            topic_id: 3,
            partition_id: 4,
            offset: 5,
        };

        let bytes = command.as_bytes();
        let consumer_id = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        let stream_id = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let topic_id = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let partition_id = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[16..24].try_into().unwrap());

        assert_eq!(bytes.is_empty(), is_empty);
        assert_eq!(consumer_id, command.consumer_id);
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partition_id, command.partition_id);
        assert_eq!(offset, command.offset);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let consumer_id = 1u32;
        let stream_id = 2u32;
        let topic_id = 3u32;
        let partition_id = 4u32;
        let offset = 5u64;
        let bytes = [
            &consumer_id.to_le_bytes(),
            &stream_id.to_le_bytes(),
            &topic_id.to_le_bytes(),
            &partition_id.to_le_bytes(),
            &offset.to_le_bytes()[0..4],
            &offset.to_le_bytes()[4..8],
        ]
        .concat();
        let command = StoreOffset::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.consumer_id, consumer_id);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, partition_id);
        assert_eq!(command.offset, offset);
    }

    #[test]
    fn should_be_read_from_string() {
        let consumer_id = 1u32;
        let stream_id = 2u32;
        let topic_id = 3u32;
        let partition_id = 4u32;
        let offset = 5u64;
        let input = format!(
            "{}|{}|{}|{}|{}",
            consumer_id, stream_id, topic_id, partition_id, offset
        );
        let command = StoreOffset::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.consumer_id, consumer_id);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, partition_id);
        assert_eq!(command.offset, offset);
    }
}
