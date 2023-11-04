use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::streams::MAX_NAME_LENGTH;
use crate::utils::text;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::{from_utf8, FromStr};

/// `UpdateStream` command is used to update an existing stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `name` - unique stream name (string), max length is 255 characters.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct UpdateStream {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique stream name (string), max length is 255 characters.
    pub name: String,
}

impl CommandPayload for UpdateStream {}

impl Default for UpdateStream {
    fn default() -> Self {
        UpdateStream {
            stream_id: Identifier::default(),
            name: "stream".to_string(),
        }
    }
}

impl Validatable<Error> for UpdateStream {
    fn validate(&self) -> Result<(), Error> {
        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(Error::InvalidStreamName);
        }

        if !text::is_resource_name_valid(&self.name) {
            return Err(Error::InvalidStreamName);
        }

        Ok(())
    }
}

impl FromStr for UpdateStream {
    type Err = Error;
    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let name = parts[1].to_string();
        let command = UpdateStream { stream_id, name };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for UpdateStream {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let mut bytes = Vec::with_capacity(1 + stream_id_bytes.len() + self.name.len());
        bytes.extend(stream_id_bytes);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.extend(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<UpdateStream, Error> {
        if bytes.len() < 5 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes)?;
        position += stream_id.get_size_bytes() as usize;
        let name_length = bytes[position];
        let name =
            from_utf8(&bytes[position + 1..position + 1 + name_length as usize])?.to_string();
        if name.len() != name_length as usize {
            return Err(Error::InvalidCommand);
        }

        let command = UpdateStream { stream_id, name };
        command.validate()?;
        Ok(command)
    }
}

impl Display for UpdateStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.stream_id, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = UpdateStream {
            stream_id: Identifier::numeric(1).unwrap(),
            name: "test".to_string(),
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(&bytes).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let name_length = bytes[position];
        let name = from_utf8(&bytes[position + 1..position + 1 + name_length as usize])
            .unwrap()
            .to_string();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let name = "test".to_string();

        let stream_id_bytes = stream_id.as_bytes();
        let mut bytes = Vec::with_capacity(1 + stream_id_bytes.len() + name.len());
        bytes.extend(stream_id_bytes);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.extend(name.as_bytes());
        let command = UpdateStream::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.name, name);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let name = "test".to_string();
        let input = format!("{}|{}", stream_id, name);
        let command = UpdateStream::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.name, name);
    }
}
