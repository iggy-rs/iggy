use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::models::user_status::UserStatus;
use crate::users::defaults::*;
use crate::utils::text;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::{from_utf8, FromStr};

/// `UpdateUser` command is used to update a user's username and status.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
/// - `username` - new username (optional), if provided, must be between 3 and 50 characters long.
/// - `status` - new status (optional)
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct UpdateUser {
    #[serde(skip)]
    pub user_id: Identifier,
    pub username: Option<String>,
    pub status: Option<UserStatus>,
}

impl CommandPayload for UpdateUser {}

impl Validatable<Error> for UpdateUser {
    fn validate(&self) -> Result<(), Error> {
        if self.username.is_none() {
            return Ok(());
        }

        let username = self.username.as_ref().unwrap();
        if username.is_empty()
            || username.len() > MAX_USERNAME_LENGTH
            || username.len() < MIN_USERNAME_LENGTH
        {
            return Err(Error::InvalidUsername);
        }

        if !text::is_resource_name_valid(username) {
            return Err(Error::InvalidUsername);
        }

        Ok(())
    }
}

impl FromStr for UpdateUser {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.is_empty() || parts.len() > 3 {
            return Err(Error::InvalidCommand);
        }

        let user_id = parts[0].parse::<Identifier>()?;
        let username = match parts.get(1) {
            Some(username) => match *username {
                "" => None,
                _ => Some(username.to_string()),
            },
            None => None,
        };
        let status = match parts.get(2) {
            Some(status) => match *status {
                "" => None,
                _ => {
                    let status = UserStatus::from_str(status)?;
                    Some(status)
                }
            },
            None => None,
        };
        let command = UpdateUser {
            user_id,
            username,
            status,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for UpdateUser {
    fn as_bytes(&self) -> Vec<u8> {
        let user_id_bytes = self.user_id.as_bytes();
        let mut bytes = Vec::new();
        bytes.extend(user_id_bytes);
        if let Some(username) = &self.username {
            bytes.put_u8(1);
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u8(username.len() as u8);
            bytes.extend(username.as_bytes());
        } else {
            bytes.put_u8(0);
        }
        if let Some(status) = &self.status {
            bytes.put_u8(1);
            bytes.put_u8(status.as_code());
        } else {
            bytes.put_u8(0);
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<UpdateUser, Error> {
        if bytes.len() < 5 {
            return Err(Error::InvalidCommand);
        }

        let user_id = Identifier::from_bytes(bytes)?;
        let mut position = user_id.get_size_bytes() as usize;
        let has_username = bytes[position];
        if has_username > 1 {
            return Err(Error::InvalidCommand);
        }

        position += 1;
        let username = if has_username == 1 {
            let username_length = bytes[position];
            position += 1;
            let username =
                from_utf8(&bytes[position..position + username_length as usize])?.to_string();
            position += username_length as usize;
            Some(username)
        } else {
            None
        };

        let has_status = bytes[position];
        if has_status > 1 {
            return Err(Error::InvalidCommand);
        }

        let status = if has_status == 1 {
            position += 1;
            let status = UserStatus::from_code(bytes[position])?;
            Some(status)
        } else {
            None
        };

        let command = UpdateUser {
            user_id,
            username,
            status,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for UpdateUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let username = self.username.as_deref().unwrap_or("");
        let status = self
            .status
            .as_ref()
            .map_or_else(String::new, |s| s.to_string());
        write!(f, "{}|{username}|{status}", self.user_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = UpdateUser {
            user_id: Identifier::numeric(1).unwrap(),
            username: Some("user".to_string()),
            status: Some(UserStatus::Active),
        };

        let bytes = command.as_bytes();
        let user_id = Identifier::from_bytes(&bytes).unwrap();
        let mut position = user_id.get_size_bytes() as usize;
        let has_username = bytes[position];
        position += 1;
        let username_length = bytes[position];
        position += 1;
        let username = from_utf8(&bytes[position..position + username_length as usize]).unwrap();
        position += username_length as usize;
        let has_status = bytes[position];
        position += 1;
        let status = UserStatus::from_code(bytes[position]).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(user_id, command.user_id);
        assert_eq!(has_username, 1);
        assert_eq!(username, command.username.unwrap());
        assert_eq!(has_status, 1);
        assert_eq!(status, command.status.unwrap());
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let user_id = Identifier::numeric(1).unwrap();
        let username = "user";
        let status = UserStatus::Active;
        let mut bytes = Vec::new();
        bytes.extend(user_id.as_bytes());
        bytes.put_u8(1);
        bytes.put_u8(username.len() as u8);
        bytes.extend(username.as_bytes());
        bytes.put_u8(1);
        bytes.put_u8(status.as_code());

        let command = UpdateUser::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
        assert_eq!(command.username.unwrap(), username);
        assert_eq!(command.status.unwrap(), status);
    }

    #[test]
    fn should_be_read_from_string() {
        let user_id = Identifier::numeric(1).unwrap();
        let username = "user";
        let status = UserStatus::Active;
        let input = format!("{user_id}|{username}|{status}");
        let command = UpdateUser::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
        assert_eq!(command.username.unwrap(), username);
        assert_eq!(command.status.unwrap(), status);
    }
}
