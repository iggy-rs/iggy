use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `GetMe` command is used to get the information connected client.
/// It has no additional payload.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct GetMe {}

impl CommandPayload for GetMe {}

impl Validatable<Error> for GetMe {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl FromStr for GetMe {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if !input.is_empty() {
            return Err(Error::InvalidCommand);
        }

        let command = GetMe {};
        command.validate()?;
        Ok(GetMe {})
    }
}

impl BytesSerializable for GetMe {
    fn as_bytes(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }

    fn from_bytes(bytes: &[u8]) -> Result<GetMe, Error> {
        if !bytes.is_empty() {
            return Err(Error::InvalidCommand);
        }

        let command = GetMe {};
        command.validate()?;
        Ok(GetMe {})
    }
}

impl Display for GetMe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_empty_bytes() {
        let command = GetMe {};
        let bytes = command.as_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn should_be_deserialized_from_empty_bytes() {
        let bytes: Vec<u8> = vec![];
        let command = GetMe::from_bytes(&bytes);
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_deserialized_from_empty_bytes() {
        let bytes: Vec<u8> = vec![0];
        let command = GetMe::from_bytes(&bytes);
        assert!(command.is_err());
    }

    #[test]
    fn should_be_read_from_empty_string() {
        let input = "";
        let command = GetMe::from_str(input);
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_read_from_non_empty_string() {
        let input = " ";
        let command = GetMe::from_str(input);
        assert!(command.is_err());
    }
}
