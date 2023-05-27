use crate::bytes_serializable::BytesSerializable;
use crate::command::GET_TOPICS;
use crate::error::Error;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize)]
pub struct GetTopics {
    pub stream_id: u32,
}

impl Validatable for GetTopics {
    fn validate(&self) -> Result<(), Error> {
        if self.stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        Ok(())
    }
}

impl FromStr for GetTopics {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 1 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<u32>()?;
        let command = GetTopics { stream_id };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for GetTopics {
    type Type = GetTopics;

    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4);
        bytes.extend(self.stream_id.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, Error> {
        if bytes.len() != 4 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes.try_into()?);
        let command = GetTopics { stream_id };
        command.validate()?;
        Ok(command)
    }
}

impl Display for GetTopics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} → stream ID: {}", GET_TOPICS, self.stream_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let is_empty = false;
        let command = GetTopics { stream_id: 1 };

        let bytes = command.as_bytes();
        let stream_id = u32::from_le_bytes(bytes[..4].try_into().unwrap());

        assert_eq!(bytes.is_empty(), is_empty);
        assert_eq!(stream_id, command.stream_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = 1u32;
        let bytes = stream_id.to_le_bytes();
        let command = GetTopics::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = 1u32;
        let input = format!("{}", stream_id);
        let command = GetTopics::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }
}
