use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::IggyError;
use crate::validatable::Validatable;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `GetUsers` command is used to retrieve the information about all users.
/// It has no additional payload.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct GetUsers {}

impl CommandPayload for GetUsers {}

impl Validatable<IggyError> for GetUsers {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for GetUsers {
    fn as_bytes(&self) -> Bytes {
        Bytes::new()
    }

    fn from_bytes(bytes: Bytes) -> Result<GetUsers, IggyError> {
        if !bytes.is_empty() {
            return Err(IggyError::InvalidCommand);
        }

        let command = GetUsers {};
        command.validate()?;
        Ok(GetUsers {})
    }
}

impl Display for GetUsers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn should_be_serialized_as_empty_bytes() {
        let command = GetUsers {};
        let bytes = command.as_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn should_be_deserialized_from_empty_bytes() {
        let command = GetUsers::from_bytes(Bytes::new());
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_deserialized_from_empty_bytes() {
        let command = GetUsers::from_bytes(Bytes::from_static(&[0]));
        assert!(command.is_err());
    }
}
