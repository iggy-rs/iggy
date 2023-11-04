use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::consumer_groups::MAX_NAME_LENGTH;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::utils::text;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::{from_utf8, FromStr};

/// `CreateConsumerGroup` command creates a new consumer group for the topic.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `consumer_group_id` - unique consumer group ID.
/// - `name` - unique consumer group name.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateConsumerGroup {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Unique consumer group ID.
    pub consumer_group_id: u32,
    /// Unique consumer group name.
    pub name: String,
}

impl CommandPayload for CreateConsumerGroup {}

impl Default for CreateConsumerGroup {
    fn default() -> Self {
        CreateConsumerGroup {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            consumer_group_id: 1,
            name: "consumer_group_1".to_string(),
        }
    }
}

impl Validatable<Error> for CreateConsumerGroup {
    fn validate(&self) -> Result<(), Error> {
        if self.consumer_group_id == 0 {
            return Err(Error::InvalidConsumerGroupId);
        }

        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(Error::InvalidConsumerGroupName);
        }

        if !text::is_resource_name_valid(&self.name) {
            return Err(Error::InvalidConsumerGroupName);
        }

        Ok(())
    }
}

impl FromStr for CreateConsumerGroup {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 4 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let topic_id = parts[1].parse::<Identifier>()?;
        let consumer_group_id = parts[2].parse::<u32>()?;
        let name = parts[3].to_string();
        let command = CreateConsumerGroup {
            stream_id,
            topic_id,
            consumer_group_id,
            name,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for CreateConsumerGroup {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let mut bytes =
            Vec::with_capacity(4 + stream_id_bytes.len() + topic_id_bytes.len() + self.name.len());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.put_u32_le(self.consumer_group_id);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.extend(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<CreateConsumerGroup, Error> {
        if bytes.len() < 10 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes)?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        position += topic_id.get_size_bytes() as usize;
        let consumer_group_id = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let name_length = bytes[position + 4];
        let name =
            from_utf8(&bytes[position + 5..position + 5 + name_length as usize])?.to_string();
        let command = CreateConsumerGroup {
            stream_id,
            topic_id,
            consumer_group_id,
            name,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for CreateConsumerGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.stream_id, self.topic_id, self.consumer_group_id, self.name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreateConsumerGroup {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            consumer_group_id: 3,
            name: "test".to_string(),
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(&bytes).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let consumer_group_id =
            u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let name_length = bytes[position + 4];
        let name = from_utf8(&bytes[position + 5..position + 5 + name_length as usize]).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(consumer_group_id, command.consumer_group_id);
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let consumer_group_id = 3u32;
        let name = "test".to_string();
        let stream_id_bytes = stream_id.as_bytes();
        let topic_id_bytes = topic_id.as_bytes();
        let mut bytes =
            Vec::with_capacity(4 + stream_id_bytes.len() + topic_id_bytes.len() + name.len());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.put_u32_le(consumer_group_id);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.extend(name.as_bytes());
        let command = CreateConsumerGroup::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.consumer_group_id, consumer_group_id);
        assert_eq!(command.name, name);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let consumer_group_id = 3u32;
        let name = "test".to_string();
        let input = format!("{stream_id}|{topic_id}|{consumer_group_id}|{name}");
        let command = CreateConsumerGroup::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.consumer_group_id, consumer_group_id);
        assert_eq!(command.name, name);
    }
}
