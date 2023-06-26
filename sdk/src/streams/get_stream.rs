use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct GetStream {
    pub stream_id: u32,
}

impl CommandPayload for GetStream {}

impl Default for GetStream {
    fn default() -> Self {
        GetStream { stream_id: 1 }
    }
}

impl Validatable for GetStream {
    fn validate(&self) -> Result<(), Error> {
        if self.stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        Ok(())
    }
}

impl FromStr for GetStream {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 1 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<u32>()?;
        let command = GetStream { stream_id };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for GetStream {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4);
        bytes.extend(self.stream_id.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<GetStream, Error> {
        if bytes.len() != 4 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes.try_into()?);
        let command = GetStream { stream_id };
        command.validate()?;
        Ok(command)
    }
}

impl Display for GetStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.stream_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = GetStream { stream_id: 1 };

        let bytes = command.as_bytes();
        let stream_id = u32::from_le_bytes(bytes[..4].try_into().unwrap());

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = 1u32;
        let bytes = stream_id.to_le_bytes();
        let command = GetStream::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = 1u32;
        let input = format!("{}", stream_id);
        let command = GetStream::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }
}
