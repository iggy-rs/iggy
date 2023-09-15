use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::models::permissions::Permissions;
use crate::models::user_status::UserStatus;
use crate::users::{
    MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH, MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
};
use crate::utils::text;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::{from_utf8, FromStr};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateUser {
    pub username: String,
    pub password: String,
    pub status: UserStatus,
    pub permissions: Option<Permissions>,
}

impl CommandPayload for CreateUser {}

impl Default for CreateUser {
    fn default() -> Self {
        CreateUser {
            username: "user".to_string(),
            password: "secret".to_string(),
            status: UserStatus::Active,
            permissions: None,
        }
    }
}

impl Validatable<Error> for CreateUser {
    fn validate(&self) -> Result<(), Error> {
        if self.username.is_empty()
            || self.username.len() > MAX_USERNAME_LENGTH
            || self.username.len() < MIN_USERNAME_LENGTH
        {
            return Err(Error::InvalidUsername);
        }

        if !text::is_resource_name_valid(&self.username) {
            return Err(Error::InvalidUsername);
        }

        if self.password.is_empty()
            || self.password.len() > MAX_PASSWORD_LENGTH
            || self.password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(Error::InvalidPassword);
        }

        Ok(())
    }
}

impl FromStr for CreateUser {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        // No support for permissions yet
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() < 2 {
            return Err(Error::InvalidCommand);
        }

        let username = parts[0].to_string();
        let password = parts[1].to_string();
        let status = match parts.get(2) {
            Some(status) => UserStatus::from_str(status)?,
            None => UserStatus::Active,
        };

        let command = CreateUser {
            username,
            password,
            status,
            permissions: None,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for CreateUser {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(2 + self.username.len() + self.password.len());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.username.len() as u8);
        bytes.extend(self.username.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.password.len() as u8);
        bytes.extend(self.password.as_bytes());
        bytes.put_u8(self.status.as_code());
        if self.permissions.is_some() {
            bytes.put_u8(1);
            // TODO: Include permissions
        } else {
            bytes.put_u8(0);
        }
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<CreateUser, Error> {
        if bytes.len() < 10 {
            return Err(Error::InvalidCommand);
        }

        let username_length = bytes[0];
        let username = from_utf8(&bytes[1..1 + username_length as usize])?.to_string();
        if username.len() != username_length as usize {
            return Err(Error::InvalidCommand);
        }

        let password_length = bytes[1 + username_length as usize];
        let password = from_utf8(
            &bytes[2 + username_length as usize
                ..2 + username_length as usize + password_length as usize],
        )?
        .to_string();
        if password.len() != password_length as usize {
            return Err(Error::InvalidCommand);
        }

        let status =
            UserStatus::from_code(bytes[2 + username_length as usize + password_length as usize])?;
        let has_permissions = bytes[3 + username_length as usize + password_length as usize];
        if has_permissions > 1 {
            return Err(Error::InvalidCommand);
        }

        // TODO: Include permissions

        let command = CreateUser {
            username,
            password,
            status,
            permissions: None,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for CreateUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let permissions = if let Some(permissions) = &self.permissions {
            permissions.to_string()
        } else {
            "no_permissions".to_string()
        };
        write!(
            f,
            "{}|{}|{}|{}",
            self.username, self.password, self.status, permissions
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreateUser {
            username: "user".to_string(),
            password: "secret".to_string(),
            status: UserStatus::Active,
            permissions: None,
        };

        let bytes = command.as_bytes();
        let username_length = bytes[0];
        let username = from_utf8(&bytes[1..1 + username_length as usize]).unwrap();
        let password_length = bytes[1 + username_length as usize];
        let password = from_utf8(
            &bytes[2 + username_length as usize
                ..2 + username_length as usize + password_length as usize],
        )
        .unwrap();
        let status =
            UserStatus::from_code(bytes[2 + username_length as usize + password_length as usize])
                .unwrap();
        let has_permissions = bytes[3 + username_length as usize + password_length as usize];

        assert!(!bytes.is_empty());
        assert_eq!(username, command.username);
        assert_eq!(password, command.password);
        assert_eq!(status, command.status);
        assert_eq!(has_permissions, 0);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let username = "user";
        let password = "secret";
        let status = UserStatus::Active;
        let has_permissions = 0u8;
        let mut bytes = Vec::new();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(username.len() as u8);
        bytes.extend(username.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(password.len() as u8);
        bytes.extend(password.as_bytes());
        bytes.put_u8(status.as_code());
        bytes.put_u8(has_permissions);
        let command = CreateUser::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.username, username);
        assert_eq!(command.password, password);
        assert_eq!(command.status, status);
        assert!(command.permissions.is_none());
    }

    #[test]
    fn should_be_read_from_string() {
        let username = "user";
        let password = "secret";
        let status = UserStatus::Active;
        let input = format!("{username}|{password}|{status}");
        let command = CreateUser::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.username, username);
        assert_eq!(command.password, password);
        assert_eq!(command.status, status);
        assert!(command.permissions.is_none());
    }
}
