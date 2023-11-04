use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `GetUsers` command is used to retrieve the information about all users.
/// It has no additional payload.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct GetUsers {}

impl CommandPayload for GetUsers {}

impl Validatable<Error> for GetUsers {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl FromStr for GetUsers {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if !input.is_empty() {
            return Err(Error::InvalidCommand);
        }

        let command = GetUsers {};
        command.validate()?;
        Ok(GetUsers {})
    }
}

impl BytesSerializable for GetUsers {
    fn as_bytes(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }

    fn from_bytes(bytes: &[u8]) -> Result<GetUsers, Error> {
        if !bytes.is_empty() {
            return Err(Error::InvalidCommand);
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
    use super::*;

    #[test]
    fn should_be_serialized_as_empty_bytes() {
        let command = GetUsers {};
        let bytes = command.as_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn should_be_deserialized_from_empty_bytes() {
        let bytes: Vec<u8> = vec![];
        let command = GetUsers::from_bytes(&bytes);
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_deserialized_from_empty_bytes() {
        let bytes: Vec<u8> = vec![0];
        let command = GetUsers::from_bytes(&bytes);
        assert!(command.is_err());
    }

    #[test]
    fn should_be_read_from_empty_string() {
        let input = "";
        let command = GetUsers::from_str(input);
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_read_from_non_empty_string() {
        let input = " ";
        let command = GetUsers::from_str(input);
        assert!(command.is_err());
    }
}
