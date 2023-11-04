use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `JoinConsumerGroup` command joins the consumer group by currently authenticated user.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `consumer_group_id` - unique consumer group ID (numeric or name).
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct JoinConsumerGroup {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Unique consumer group ID (numeric or name).
    #[serde(skip)]
    pub consumer_group_id: Identifier,
}

impl CommandPayload for JoinConsumerGroup {}

impl Validatable<Error> for JoinConsumerGroup {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl FromStr for JoinConsumerGroup {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 3 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let topic_id = parts[1].parse::<Identifier>()?;
        let consumer_group_id = parts[2].parse::<Identifier>()?;
        let command = JoinConsumerGroup {
            stream_id,
            topic_id,
            consumer_group_id,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for JoinConsumerGroup {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let consumer_group_id_bytes = self.consumer_group_id.as_bytes();
        let mut bytes = Vec::with_capacity(
            stream_id_bytes.len() + topic_id_bytes.len() + consumer_group_id_bytes.len(),
        );
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(consumer_group_id_bytes);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<JoinConsumerGroup, Error> {
        if bytes.len() < 9 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes)?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        position += topic_id.get_size_bytes() as usize;
        let consumer_group_id = Identifier::from_bytes(&bytes[position..])?;
        let command = JoinConsumerGroup {
            stream_id,
            topic_id,
            consumer_group_id,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for JoinConsumerGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}",
            self.stream_id, self.topic_id, self.consumer_group_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = JoinConsumerGroup {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            consumer_group_id: Identifier::numeric(3).unwrap(),
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(&bytes).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let consumer_group_id = Identifier::from_bytes(&bytes[position..]).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(consumer_group_id, command.consumer_group_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let consumer_group_id = Identifier::numeric(3).unwrap();
        let stream_id_bytes = stream_id.as_bytes();
        let topic_id_bytes = topic_id.as_bytes();
        let consumer_group_id_bytes = consumer_group_id.as_bytes();
        let mut bytes = Vec::with_capacity(
            stream_id_bytes.len() + topic_id_bytes.len() + consumer_group_id_bytes.len(),
        );
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(consumer_group_id_bytes);
        let command = JoinConsumerGroup::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.consumer_group_id, consumer_group_id);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let consumer_group_id = Identifier::numeric(3).unwrap();
        let input = format!("{stream_id}|{topic_id}|{consumer_group_id}");
        let command = JoinConsumerGroup::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.consumer_group_id, consumer_group_id);
    }
}
