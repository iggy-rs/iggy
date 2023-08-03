use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::consumer_type::ConsumerType;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct StoreConsumerOffset {
    #[serde(default = "default_consumer_type")]
    pub consumer_type: ConsumerType,
    #[serde(default = "default_consumer_id")]
    pub consumer_id: u32,
    #[serde(skip)]
    pub stream_id: Identifier,
    #[serde(skip)]
    pub topic_id: Identifier,
    pub partition_id: u32,
    pub offset: u64,
}

impl Default for StoreConsumerOffset {
    fn default() -> Self {
        StoreConsumerOffset {
            consumer_type: default_consumer_type(),
            consumer_id: default_consumer_id(),
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 1,
            offset: 0,
        }
    }
}

impl CommandPayload for StoreConsumerOffset {}

fn default_consumer_type() -> ConsumerType {
    ConsumerType::Consumer
}

fn default_consumer_id() -> u32 {
    0
}

impl Validatable for StoreConsumerOffset {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl FromStr for StoreConsumerOffset {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 6 {
            return Err(Error::InvalidCommand);
        }

        let consumer_type = ConsumerType::from_str(parts[0])?;
        let consumer_id = parts[1].parse::<u32>()?;
        let stream_id = parts[2].parse::<Identifier>()?;
        let topic_id = parts[3].parse::<Identifier>()?;
        let partition_id = parts[4].parse::<u32>()?;
        let offset = parts[5].parse::<u64>()?;
        let command = StoreConsumerOffset {
            consumer_type,
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

impl BytesSerializable for StoreConsumerOffset {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(17 + stream_id_bytes.len() + topic_id_bytes.len());
        bytes.extend(self.consumer_type.as_code().to_le_bytes());
        bytes.extend(self.consumer_id.to_le_bytes());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(self.partition_id.to_le_bytes());
        bytes.extend(self.offset.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<StoreConsumerOffset, Error> {
        if bytes.len() < 23 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let consumer_type = ConsumerType::from_code(bytes[0])?;
        let consumer_id = u32::from_le_bytes(bytes[1..5].try_into()?);
        position += 5;
        let stream_id = Identifier::from_bytes(&bytes[position..])?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        position += topic_id.get_size_bytes() as usize;
        let partition_id = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let offset = u64::from_le_bytes(bytes[position + 4..position + 12].try_into()?);
        let command = StoreConsumerOffset {
            consumer_type,
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

impl Display for StoreConsumerOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}",
            self.consumer_type,
            self.consumer_id,
            self.stream_id,
            self.topic_id,
            self.partition_id,
            self.offset
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = StoreConsumerOffset {
            consumer_type: ConsumerType::Consumer,
            consumer_id: 1,
            stream_id: Identifier::numeric(2).unwrap(),
            topic_id: Identifier::numeric(3).unwrap(),
            partition_id: 4,
            offset: 5,
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let consumer_type = ConsumerType::from_code(bytes[0]).unwrap();
        let consumer_id = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
        position += 5;
        let stream_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let partition_id = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[position + 4..position + 12].try_into().unwrap());

        assert!(!bytes.is_empty());
        assert_eq!(consumer_type, command.consumer_type);
        assert_eq!(consumer_id, command.consumer_id);
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partition_id, command.partition_id);
        assert_eq!(offset, command.offset);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let consumer_type = ConsumerType::Consumer;
        let consumer_id = 1u32;
        let stream_id = Identifier::numeric(2).unwrap();
        let topic_id = Identifier::numeric(3).unwrap();
        let partition_id = 4u32;
        let offset = 5u64;

        let stream_id_bytes = stream_id.as_bytes();
        let topic_id_bytes = topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(17 + stream_id_bytes.len() + topic_id_bytes.len());
        bytes.extend(consumer_type.as_code().to_le_bytes());
        bytes.extend(consumer_id.to_le_bytes());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(partition_id.to_le_bytes());
        bytes.extend(offset.to_le_bytes());

        let command = StoreConsumerOffset::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.consumer_type, consumer_type);
        assert_eq!(command.consumer_id, consumer_id);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, partition_id);
        assert_eq!(command.offset, offset);
    }

    #[test]
    fn should_be_read_from_string() {
        let consumer_type = ConsumerType::Consumer;
        let consumer_id = 1u32;
        let stream_id = Identifier::numeric(2).unwrap();
        let topic_id = Identifier::numeric(3).unwrap();
        let partition_id = 4u32;
        let offset = 5u64;
        let input = format!(
            "{}|{}|{}|{}|{}|{}",
            consumer_type, consumer_id, stream_id, topic_id, partition_id, offset
        );
        let command = StoreConsumerOffset::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.consumer_type, consumer_type);
        assert_eq!(command.consumer_id, consumer_id);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, partition_id);
        assert_eq!(command.offset, offset);
    }
}
