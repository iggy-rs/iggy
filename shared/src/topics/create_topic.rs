use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use std::str::{from_utf8, FromStr};

pub const MAX_NAME_LENGTH: usize = 100;
pub const MAX_PARTITIONS_COUNT: u32 = 100000;

#[derive(Debug)]
pub struct CreateTopic {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partitions_count: u32,
    pub name: String,
}

impl FromStr for CreateTopic {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 4 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<u32>()?;
        if stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        let topic_id = parts[1].parse::<u32>()?;
        if topic_id == 0 {
            return Err(Error::InvalidTopicId);
        }

        let partitions_count = parts[2].parse::<u32>()?;
        let name = parts[3].to_string();

        if !(1..=MAX_PARTITIONS_COUNT).contains(&partitions_count) {
            return Err(Error::InvalidTopicPartitions);
        }

        if name.len() > MAX_NAME_LENGTH {
            return Err(Error::InvalidTopicName);
        }

        Ok(CreateTopic {
            stream_id,
            topic_id,
            partitions_count,
            name,
        })
    }
}

impl BytesSerializable for CreateTopic {
    type Type = CreateTopic;

    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12 + self.name.len());
        bytes.extend(self.stream_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        bytes.extend(self.partitions_count.to_le_bytes());
        bytes.extend(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, Error> {
        if bytes.len() < 13 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes[..4].try_into()?);
        if stream_id == 0 {
            return Err(Error::InvalidTopicId);
        }

        let topic_id = u32::from_le_bytes(bytes[4..8].try_into()?);
        if topic_id == 0 {
            return Err(Error::InvalidTopicId);
        }

        let partitions_count = u32::from_le_bytes(bytes[8..12].try_into()?);
        let name = from_utf8(&bytes[12..])?.to_string();

        if !(1..=MAX_PARTITIONS_COUNT).contains(&partitions_count) {
            return Err(Error::InvalidTopicPartitions);
        }

        if name.len() > MAX_NAME_LENGTH {
            return Err(Error::InvalidTopicName);
        }

        Ok(CreateTopic {
            stream_id,
            topic_id,
            partitions_count,
            name,
        })
    }
}
