use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::{from_utf8, FromStr};

const MAX_NAME_LENGTH: usize = 1000;
const MAX_PARTITIONS_COUNT: u32 = 100000;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateTopic {
    #[serde(skip)]
    pub stream_id: u32,
    pub topic_id: u32,
    pub partitions_count: u32,
    pub name: String,
}

impl CommandPayload for CreateTopic {}

impl Default for CreateTopic {
    fn default() -> Self {
        CreateTopic {
            stream_id: 1,
            topic_id: 1,
            partitions_count: 1,
            name: "topic".to_string(),
        }
    }
}

impl Validatable for CreateTopic {
    fn validate(&self) -> Result<(), Error> {
        if self.stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        if self.topic_id == 0 {
            return Err(Error::InvalidTopicId);
        }

        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(Error::InvalidTopicName);
        }

        if !(1..=MAX_PARTITIONS_COUNT).contains(&self.partitions_count) {
            return Err(Error::InvalidTopicPartitions);
        }

        Ok(())
    }
}

impl FromStr for CreateTopic {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 4 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<u32>()?;
        let topic_id = parts[1].parse::<u32>()?;
        let partitions_count = parts[2].parse::<u32>()?;
        let name = parts[3].to_string();
        let command = CreateTopic {
            stream_id,
            topic_id,
            partitions_count,
            name,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for CreateTopic {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12 + self.name.len());
        bytes.extend(self.stream_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        bytes.extend(self.partitions_count.to_le_bytes());
        bytes.extend(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<CreateTopic, Error> {
        if bytes.len() < 13 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes[..4].try_into()?);
        let topic_id = u32::from_le_bytes(bytes[4..8].try_into()?);
        let partitions_count = u32::from_le_bytes(bytes[8..12].try_into()?);
        let name = from_utf8(&bytes[12..])?.to_string();
        let command = CreateTopic {
            stream_id,
            topic_id,
            partitions_count,
            name,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for CreateTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.stream_id, self.topic_id, self.partitions_count, self.name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreateTopic {
            stream_id: 1,
            topic_id: 2,
            partitions_count: 3,
            name: "test".to_string(),
        };

        let bytes = command.as_bytes();
        let stream_id = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        let topic_id = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let partitions_count = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let name = from_utf8(&bytes[12..]).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partitions_count, command.partitions_count);
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = 1u32;
        let topic_id = 2u32;
        let partitions_count = 3u32;
        let name = "test".to_string();
        let bytes = [
            &stream_id.to_le_bytes(),
            &topic_id.to_le_bytes(),
            &partitions_count.to_le_bytes(),
            name.as_bytes(),
        ]
        .concat();
        let command = CreateTopic::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partitions_count, partitions_count);
        assert_eq!(command.name, name);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = 1u32;
        let topic_id = 2u32;
        let partitions_count = 3u32;
        let name = "test".to_string();
        let input = format!("{}|{}|{}|{}", stream_id, topic_id, partitions_count, name);
        let command = CreateTopic::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partitions_count, partitions_count);
        assert_eq!(command.name, name);
    }
}
