use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `GetConsumerGroup` command retrieves the consumer group from the topic.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `consumer_group_id` - unique consumer group ID (numeric or name).
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct GetConsumerGroup {
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

impl CommandPayload for GetConsumerGroup {}

impl Validatable<IggyError> for GetConsumerGroup {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for GetConsumerGroup {
    fn as_bytes(&self) -> Bytes {
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let consumer_group_id_bytes = self.consumer_group_id.as_bytes();
        let mut bytes = BytesMut::with_capacity(
            stream_id_bytes.len() + topic_id_bytes.len() + consumer_group_id_bytes.len(),
        );
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(consumer_group_id_bytes);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<GetConsumerGroup, IggyError> {
        if bytes.len() < 9 {
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone())?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(bytes.slice(position..))?;
        position += topic_id.get_size_bytes() as usize;
        let consumer_group_id = Identifier::from_bytes(bytes.slice(position..))?;
        let command = GetConsumerGroup {
            stream_id,
            topic_id,
            consumer_group_id,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for GetConsumerGroup {
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
        let command = GetConsumerGroup {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            consumer_group_id: Identifier::numeric(3).unwrap(),
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone()).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let consumer_group_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();

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
        let mut bytes = BytesMut::with_capacity(
            stream_id_bytes.len() + topic_id_bytes.len() + consumer_group_id_bytes.len(),
        );
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(consumer_group_id_bytes);
        let command = GetConsumerGroup::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.consumer_group_id, consumer_group_id);
    }
}
