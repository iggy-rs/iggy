use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DeleteGroup {
    pub stream_id: u32,
    pub topic_id: u32,
    pub group_id: u32,
}

impl CommandPayload for DeleteGroup {}

impl Default for DeleteGroup {
    fn default() -> Self {
        DeleteGroup {
            stream_id: 1,
            topic_id: 1,
            group_id: 1,
        }
    }
}

impl Validatable for DeleteGroup {
    fn validate(&self) -> Result<(), Error> {
        if self.stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        if self.topic_id == 0 {
            return Err(Error::InvalidTopicId);
        }

        if self.group_id == 0 {
            return Err(Error::InvalidConsumerGroupId);
        }

        Ok(())
    }
}

impl FromStr for DeleteGroup {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 3 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<u32>()?;
        let topic_id = parts[1].parse::<u32>()?;
        let group_id = parts[2].parse::<u32>()?;
        let command = DeleteGroup {
            stream_id,
            topic_id,
            group_id,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for DeleteGroup {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12);
        bytes.extend(self.stream_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        bytes.extend(self.group_id.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<DeleteGroup, Error> {
        if bytes.len() != 12 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes[..4].try_into()?);
        let topic_id = u32::from_le_bytes(bytes[4..8].try_into()?);
        let group_id = u32::from_le_bytes(bytes[8..12].try_into()?);
        let command = DeleteGroup {
            stream_id,
            topic_id,
            group_id,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for DeleteGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}|{}", self.stream_id, self.topic_id, self.group_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = DeleteGroup {
            stream_id: 1,
            topic_id: 2,
            group_id: 3,
        };

        let bytes = command.as_bytes();
        let stream_id = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        let topic_id = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let group_id = u32::from_le_bytes(bytes[8..12].try_into().unwrap());

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(group_id, command.group_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = 1u32;
        let topic_id = 2u32;
        let group_id = 3u32;
        let bytes = [
            stream_id.to_le_bytes(),
            topic_id.to_le_bytes(),
            group_id.to_le_bytes(),
        ]
        .concat();
        let command = DeleteGroup::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.group_id, group_id);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = 1u32;
        let topic_id = 2u32;
        let group_id = 3u32;
        let input = format!("{}|{}|{}", stream_id, topic_id, group_id);
        let command = DeleteGroup::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.group_id, group_id);
    }
}
