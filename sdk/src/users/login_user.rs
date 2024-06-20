use crate::bytes_serializable::BytesSerializable;
use crate::command::{CommandPayload, HashableCommand};
use crate::error::IggyError;
use crate::users::defaults::*;
use crate::utils::text;
use crate::validatable::Validatable;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::from_utf8;

/// `LoginUser` command is used to login a user by username and password.
/// It has additional payload:
/// - `username` - username, must be between 3 and 50 characters long.
/// - `password` - password, must be between 3 and 100 characters long.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct LoginUser {
    /// Username, must be between 3 and 50 characters long.
    pub username: String,
    /// Password, must be between 3 and 100 characters long.
    pub password: String,
}

impl CommandPayload for LoginUser {}
impl HashableCommand for LoginUser {
    fn hash(&self) -> Option<u32> {
        None
    }
}

impl Default for LoginUser {
    fn default() -> Self {
        LoginUser {
            username: "user".to_string(),
            password: "secret".to_string(),
        }
    }
}

impl Validatable<IggyError> for LoginUser {
    fn validate(&self) -> Result<(), IggyError> {
        if self.username.is_empty()
            || self.username.len() > MAX_USERNAME_LENGTH
            || self.username.len() < MIN_USERNAME_LENGTH
        {
            return Err(IggyError::InvalidUsername);
        }

        if !text::is_resource_name_valid(&self.username) {
            return Err(IggyError::InvalidUsername);
        }

        if self.password.is_empty()
            || self.password.len() > MAX_PASSWORD_LENGTH
            || self.password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(IggyError::InvalidPassword);
        }

        Ok(())
    }
}

impl BytesSerializable for LoginUser {
    fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(2 + self.username.len() + self.password.len());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.username.len() as u8);
        bytes.put_slice(self.username.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.password.len() as u8);
        bytes.put_slice(self.password.as_bytes());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<LoginUser, IggyError> {
        if bytes.len() < 4 {
            return Err(IggyError::InvalidCommand);
        }

        let username_length = bytes[0];
        let username = from_utf8(&bytes[1..=(username_length as usize)])?.to_string();
        if username.len() != username_length as usize {
            return Err(IggyError::InvalidCommand);
        }

        let password_length = bytes[1 + username_length as usize];
        let password = from_utf8(
            &bytes[2 + username_length as usize
                ..2 + username_length as usize + password_length as usize],
        )?
        .to_string();
        if password.len() != password_length as usize {
            return Err(IggyError::InvalidCommand);
        }

        let command = LoginUser { username, password };
        command.validate()?;
        Ok(command)
    }
}

impl Display for LoginUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.username, self.password)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = LoginUser {
            username: "user".to_string(),
            password: "secret".to_string(),
        };

        let bytes = command.as_bytes();
        let username_length = bytes[0];
        let username = from_utf8(&bytes[1..=(username_length as usize)]).unwrap();
        let password_length = bytes[1 + username_length as usize];
        let password = from_utf8(
            &bytes[2 + username_length as usize
                ..2 + username_length as usize + password_length as usize],
        )
        .unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(username, command.username);
        assert_eq!(password, command.password);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let username = "user";
        let password = "secret";
        let mut bytes = BytesMut::new();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(username.len() as u8);
        bytes.put_slice(username.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(password.len() as u8);
        bytes.put_slice(password.as_bytes());
        let command = LoginUser::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.username, username);
        assert_eq!(command.password, password);
    }
}
