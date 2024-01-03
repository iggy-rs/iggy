use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `PurgeStream` command is used to purge stream data (all the messages from its topics).
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct PurgeStream {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
}

impl CommandPayload for PurgeStream {}

impl Validatable<Error> for PurgeStream {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl FromStr for PurgeStream {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 1 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let command = PurgeStream { stream_id };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for PurgeStream {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let mut bytes = Vec::with_capacity(stream_id_bytes.len());
        bytes.extend(stream_id_bytes);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<PurgeStream, Error> {
        if bytes.len() < 5 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = Identifier::from_bytes(bytes)?;
        let command = PurgeStream { stream_id };
        command.validate()?;
        Ok(command)
    }
}

impl Display for PurgeStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.stream_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = PurgeStream {
            stream_id: Identifier::numeric(1).unwrap(),
        };

        let bytes = command.as_bytes();
        let stream_id = Identifier::from_bytes(&bytes).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let bytes = stream_id.as_bytes();
        let command = PurgeStream::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let input = format!("{stream_id}");
        let command = PurgeStream::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }
}
