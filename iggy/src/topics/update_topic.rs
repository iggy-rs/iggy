use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::topics::MAX_NAME_LENGTH;
use crate::utils::text;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::{from_utf8, FromStr};

/// `UpdateTopic` command is used to update a topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `message_expiry` - message expiry in seconds (optional), if `None` then messages will never expire.
/// - `name` - unique topic name, max length is 255 characters.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct UpdateTopic {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Message expiry in seconds (optional), if `None` then messages will never expire.
    pub message_expiry: Option<u32>,
    /// Unique topic name, max length is 255 characters.
    pub name: String,
}

impl CommandPayload for UpdateTopic {}

impl Default for UpdateTopic {
    fn default() -> Self {
        UpdateTopic {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            message_expiry: None,
            name: "topic".to_string(),
        }
    }
}

impl Validatable<Error> for UpdateTopic {
    fn validate(&self) -> Result<(), Error> {
        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(Error::InvalidTopicName);
        }

        if !text::is_resource_name_valid(&self.name) {
            return Err(Error::InvalidTopicName);
        }

        Ok(())
    }
}

impl FromStr for UpdateTopic {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 4 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let topic_id = parts[1].parse::<Identifier>()?;
        let message_expiry = parts[2].parse::<u32>();
        let message_expiry = match message_expiry {
            Ok(message_expiry) => match message_expiry {
                0 => None,
                _ => Some(message_expiry),
            },
            Err(_) => None,
        };
        let name = parts[3].to_string();
        let command = UpdateTopic {
            stream_id,
            topic_id,
            message_expiry,
            name,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for UpdateTopic {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let mut bytes =
            Vec::with_capacity(5 + stream_id_bytes.len() + topic_id_bytes.len() + self.name.len());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        match self.message_expiry {
            Some(message_expiry) => bytes.put_u32_le(message_expiry),
            None => bytes.put_u32_le(0),
        }
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.extend(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<UpdateTopic, Error> {
        if bytes.len() < 12 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes)?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        position += topic_id.get_size_bytes() as usize;
        let message_expiry = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let message_expiry = match message_expiry {
            0 => None,
            _ => Some(message_expiry),
        };
        let name_length = bytes[position + 4];
        let name =
            from_utf8(&bytes[position + 5..position + 5 + name_length as usize])?.to_string();
        if name.len() != name_length as usize {
            return Err(Error::InvalidCommand);
        }
        let command = UpdateTopic {
            stream_id,
            topic_id,
            message_expiry,
            name,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for UpdateTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.stream_id,
            self.topic_id,
            self.message_expiry.unwrap_or(0),
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
        let command = UpdateTopic {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            message_expiry: Some(10),
            name: "test".to_string(),
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(&bytes).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let message_expiry = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let message_expiry = match message_expiry {
            0 => None,
            _ => Some(message_expiry),
        };
        let name_length = bytes[position + 4];
        let name = from_utf8(&bytes[position + 5..position + 5 + name_length as usize])
            .unwrap()
            .to_string();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(message_expiry, command.message_expiry);
        assert_eq!(name.len() as u8, command.name.len() as u8);
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let name = "test".to_string();
        let message_expiry = 10;

        let stream_id_bytes = stream_id.as_bytes();
        let topic_id_bytes = topic_id.as_bytes();
        let mut bytes =
            Vec::with_capacity(5 + stream_id_bytes.len() + topic_id_bytes.len() + name.len());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.put_u32_le(message_expiry);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.extend(name.as_bytes());

        let command = UpdateTopic::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.message_expiry, Some(message_expiry));
        assert_eq!(command.name, name);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let message_expiry = 10;
        let name = "test".to_string();
        let input = format!("{}|{}|{}|{}", stream_id, topic_id, message_expiry, name);
        let command = UpdateTopic::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.message_expiry, Some(message_expiry));
        assert_eq!(command.name, name);
    }
}
