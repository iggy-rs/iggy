use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::users::defaults::*;
use crate::utils::text;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::{from_utf8, FromStr};

/// `DeletePersonalAccessToken` command is used to delete a personal access token for the authenticated user.
/// It has additional payload:
/// - `name` - unique name of the token, must be between 3 and 3 characters long.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DeletePersonalAccessToken {
    /// Unique name of the token, must be between 3 and 3 characters long.
    pub name: String,
}

impl CommandPayload for DeletePersonalAccessToken {}

impl Default for DeletePersonalAccessToken {
    fn default() -> Self {
        DeletePersonalAccessToken {
            name: "token".to_string(),
        }
    }
}

impl Validatable<Error> for DeletePersonalAccessToken {
    fn validate(&self) -> Result<(), Error> {
        if self.name.is_empty()
            || self.name.len() > MAX_PERSONAL_ACCESS_TOKEN_NAME_LENGTH
            || self.name.len() < MIN_PERSONAL_ACCESS_TOKEN_NAME_LENGTH
        {
            return Err(Error::InvalidPersonalAccessTokenName);
        }

        if !text::is_resource_name_valid(&self.name) {
            return Err(Error::InvalidPersonalAccessTokenName);
        }

        Ok(())
    }
}

impl FromStr for DeletePersonalAccessToken {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 1 {
            return Err(Error::InvalidCommand);
        }

        let name = parts[0].to_string();
        let command = DeletePersonalAccessToken { name };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for DeletePersonalAccessToken {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(5 + self.name.len());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.extend(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<DeletePersonalAccessToken, Error> {
        if bytes.len() < 4 {
            return Err(Error::InvalidCommand);
        }

        let name_length = bytes[0];
        let name = from_utf8(&bytes[1..1 + name_length as usize])?.to_string();
        if name.len() != name_length as usize {
            return Err(Error::InvalidCommand);
        }

        let command = DeletePersonalAccessToken { name };
        command.validate()?;
        Ok(command)
    }
}

impl Display for DeletePersonalAccessToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = DeletePersonalAccessToken {
            name: "test".to_string(),
        };

        let bytes = command.as_bytes();
        let name_length = bytes[0];
        let name = from_utf8(&bytes[1..1 + name_length as usize]).unwrap();
        assert!(!bytes.is_empty());
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let name = "test";
        let mut bytes = Vec::new();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.extend(name.as_bytes());

        let command = DeletePersonalAccessToken::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.name, name);
    }

    #[test]
    fn should_be_read_from_string() {
        let name = "test";
        let input = name;
        let command = DeletePersonalAccessToken::from_str(input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.name, name);
    }
}
