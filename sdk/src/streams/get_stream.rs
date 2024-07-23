use crate::bytes_serializable::BytesSerializable;
use crate::command::{Command, GET_STREAM_CODE};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `GetStream` command is used to retrieve the information about a stream by unique ID.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct GetStream {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
}

impl Command for GetStream {
    fn code(&self) -> u32 {
        GET_STREAM_CODE
    }
}

impl Validatable<IggyError> for GetStream {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for GetStream {
    fn to_bytes(&self) -> Bytes {
        self.stream_id.to_bytes()
    }

    fn from_bytes(bytes: Bytes) -> std::result::Result<GetStream, IggyError> {
        if bytes.len() < 3 {
            return Err(IggyError::InvalidCommand);
        }

        let stream_id = Identifier::from_bytes(bytes)?;
        let command = GetStream { stream_id };
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
        let command = GetStream {
            stream_id: Identifier::numeric(1).unwrap(),
        };

        let bytes = command.to_bytes();
        let stream_id = Identifier::from_bytes(bytes.clone()).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let bytes = stream_id.to_bytes();
        let command = GetStream::from_bytes(bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }
}
