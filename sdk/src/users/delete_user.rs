use crate::bytes_serializable::BytesSerializable;
use crate::command::{Command, DELETE_USER_CODE};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `DeleteUser` command is used to delete a user by unique ID.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct DeleteUser {
    /// Unique user ID (numeric or name).
    #[serde(skip)]
    pub user_id: Identifier,
}

impl Command for DeleteUser {
    fn code(&self) -> u32 {
        DELETE_USER_CODE
    }
}

impl Validatable<IggyError> for DeleteUser {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for DeleteUser {
    fn to_bytes(&self) -> Bytes {
        self.user_id.to_bytes()
    }

    fn from_bytes(bytes: Bytes) -> Result<DeleteUser, IggyError> {
        if bytes.len() < 3 {
            return Err(IggyError::InvalidCommand);
        }

        let user_id = Identifier::from_bytes(bytes)?;
        let command = DeleteUser { user_id };
        Ok(command)
    }
}

impl Display for DeleteUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.user_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = DeleteUser {
            user_id: Identifier::numeric(1).unwrap(),
        };

        let bytes = command.to_bytes();
        let user_id = Identifier::from_bytes(bytes.clone()).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(user_id, command.user_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let user_id = Identifier::numeric(1).unwrap();
        let bytes = user_id.to_bytes();
        let command = DeleteUser::from_bytes(bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
    }
}
