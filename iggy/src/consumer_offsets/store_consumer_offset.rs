use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::consumer::{Consumer, ConsumerKind};
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `StoreConsumerOffset` command stores the offset of a consumer for a given partition on the server.
/// It has additional payload:
/// - `consumer` - the consumer that is storing the offset, either the regular consumer or the consumer group.
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `partition_id` - partition ID on which the offset is stored. Has to be specified for the regular consumer. For consumer group it is ignored (use `None`).
/// - `offset` - offset to store.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct StoreConsumerOffset {
    /// The consumer that is storing the offset, either the regular consumer or the consumer group.
    #[serde(flatten)]
    pub consumer: Consumer,
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Partition ID on which the offset is stored. Has to be specified for the regular consumer. For consumer group it is ignored (use `None`).
    pub partition_id: Option<u32>,
    /// Offset to store.
    pub offset: u64,
}

impl Default for StoreConsumerOffset {
    fn default() -> Self {
        StoreConsumerOffset {
            consumer: Consumer::default(),
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: Some(1),
            offset: 0,
        }
    }
}

impl CommandPayload for StoreConsumerOffset {}

impl Validatable<Error> for StoreConsumerOffset {
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

        let consumer_kind = ConsumerKind::from_str(parts[0])?;
        let consumer_id = parts[1].parse::<Identifier>()?;
        let consumer = Consumer {
            kind: consumer_kind,
            id: consumer_id,
        };
        let stream_id = parts[2].parse::<Identifier>()?;
        let topic_id = parts[3].parse::<Identifier>()?;
        let partition_id = parts[4].parse::<u32>()?;
        let offset = parts[5].parse::<u64>()?;
        let command = StoreConsumerOffset {
            consumer,
            stream_id,
            topic_id,
            partition_id: Some(partition_id),
            offset,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for StoreConsumerOffset {
    fn as_bytes(&self) -> Vec<u8> {
        let consumer_bytes = self.consumer.as_bytes();
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(
            12 + consumer_bytes.len() + stream_id_bytes.len() + topic_id_bytes.len(),
        );
        bytes.extend(consumer_bytes);
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        if let Some(partition_id) = self.partition_id {
            bytes.put_u32_le(partition_id);
        } else {
            bytes.put_u32_le(0);
        }
        bytes.put_u64_le(self.offset);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<StoreConsumerOffset, Error> {
        if bytes.len() < 23 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let consumer_kind = ConsumerKind::from_code(bytes[0])?;
        let consumer_id = Identifier::from_bytes(&bytes[1..])?;
        position += 1 + consumer_id.get_size_bytes() as usize;
        let consumer = Consumer {
            kind: consumer_kind,
            id: consumer_id,
        };
        let stream_id = Identifier::from_bytes(&bytes[position..])?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        position += topic_id.get_size_bytes() as usize;
        let partition_id = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let partition_id = if partition_id == 0 {
            None
        } else {
            Some(partition_id)
        };
        let offset = u64::from_le_bytes(bytes[position + 4..position + 12].try_into()?);
        let command = StoreConsumerOffset {
            consumer,
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
            "{}|{}|{}|{}|{}",
            self.consumer,
            self.stream_id,
            self.topic_id,
            self.partition_id.unwrap_or(0),
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
            consumer: Consumer::new(Identifier::numeric(1).unwrap()),
            stream_id: Identifier::numeric(2).unwrap(),
            topic_id: Identifier::numeric(3).unwrap(),
            partition_id: Some(4),
            offset: 5,
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let consumer_kind = ConsumerKind::from_code(bytes[0]).unwrap();
        let consumer_id = Identifier::from_bytes(&bytes[1..]).unwrap();
        position += 1 + consumer_id.get_size_bytes() as usize;
        let consumer = Consumer {
            kind: consumer_kind,
            id: consumer_id,
        };
        let stream_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let partition_id = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[position + 4..position + 12].try_into().unwrap());

        assert!(!bytes.is_empty());
        assert_eq!(consumer, command.consumer);
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(Some(partition_id), command.partition_id);
        assert_eq!(offset, command.offset);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let consumer = Consumer::new(Identifier::numeric(1).unwrap());
        let stream_id = Identifier::numeric(2).unwrap();
        let topic_id = Identifier::numeric(3).unwrap();
        let partition_id = 4u32;
        let offset = 5u64;

        let consumer_bytes = consumer.as_bytes();
        let stream_id_bytes = stream_id.as_bytes();
        let topic_id_bytes = topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(
            12 + consumer_bytes.len() + stream_id_bytes.len() + topic_id_bytes.len(),
        );
        bytes.extend(consumer_bytes);
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.put_u32_le(partition_id);
        bytes.put_u64_le(offset);

        let command = StoreConsumerOffset::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.consumer, consumer);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, Some(partition_id));
        assert_eq!(command.offset, offset);
    }

    #[test]
    fn should_be_read_from_string() {
        let consumer = Consumer::new(Identifier::numeric(1).unwrap());
        let stream_id = Identifier::numeric(2).unwrap();
        let topic_id = Identifier::numeric(3).unwrap();
        let partition_id = 4u32;
        let offset = 5u64;
        let input = format!("{consumer}|{stream_id}|{topic_id}|{partition_id}|{offset}");
        let command = StoreConsumerOffset::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.consumer, consumer);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, Some(partition_id));
        assert_eq!(command.offset, offset);
    }
}
