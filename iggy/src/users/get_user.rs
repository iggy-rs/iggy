use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `GetUser` command is used to retrieve the information about a user by unique ID.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct GetUser {
    #[serde(skip)]
    /// Unique user ID (numeric or name).
    pub user_id: Identifier,
}

impl CommandPayload for GetUser {}

impl Validatable<Error> for GetUser {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl FromStr for GetUser {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 1 {
            return Err(Error::InvalidCommand);
        }

        let user_id = parts[0].parse::<Identifier>()?;
        let command = GetUser { user_id };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for GetUser {
    fn as_bytes(&self) -> Vec<u8> {
        self.user_id.as_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<GetUser, Error> {
        if bytes.len() < 3 {
            return Err(Error::InvalidCommand);
        }

        let user_id = Identifier::from_bytes(bytes)?;
        let command = GetUser { user_id };
        command.validate()?;
        Ok(command)
    }
}

impl Display for GetUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.user_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = GetUser {
            user_id: Identifier::numeric(1).unwrap(),
        };

        let bytes = command.as_bytes();
        let user_id = Identifier::from_bytes(&bytes).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(user_id, command.user_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let user_id = Identifier::numeric(1).unwrap();
        let bytes = user_id.as_bytes();
        let command = GetUser::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
    }

    #[test]
    fn should_be_read_from_string() {
        let user_id = Identifier::numeric(1).unwrap();
        let input = user_id.to_string();
        let command = GetUser::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
    }
}
