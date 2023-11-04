use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::models::permissions::Permissions;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `UpdatePermissions` command is used to update a user's permissions.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
/// - `permissions` - new permissions (optional)
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct UpdatePermissions {
    /// Unique user ID (numeric or name).
    #[serde(skip)]
    pub user_id: Identifier,
    /// New permissions if `None` is provided, then the existing user's permissions will be removed.
    pub permissions: Option<Permissions>,
}

impl CommandPayload for UpdatePermissions {}

impl Validatable<Error> for UpdatePermissions {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl FromStr for UpdatePermissions {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() > 2 {
            return Err(Error::InvalidCommand);
        }

        // No support for permissions yet
        let user_id = parts[0].parse::<Identifier>()?;
        let command = UpdatePermissions {
            user_id,
            permissions: None,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for UpdatePermissions {
    fn as_bytes(&self) -> Vec<u8> {
        let user_id_bytes = self.user_id.as_bytes();
        let mut bytes = Vec::new();
        bytes.extend(user_id_bytes);
        if let Some(permissions) = &self.permissions {
            bytes.put_u8(1);
            bytes.put_u32_le(permissions.as_bytes().len() as u32);
            bytes.extend(permissions.as_bytes());
        } else {
            bytes.put_u8(0);
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<UpdatePermissions, Error> {
        if bytes.len() < 4 {
            return Err(Error::InvalidCommand);
        }

        let user_id = Identifier::from_bytes(bytes)?;
        let mut position = user_id.get_size_bytes() as usize;
        let has_permissions = bytes[position];
        if has_permissions > 1 {
            return Err(Error::InvalidCommand);
        }

        position += 1;
        let permissions = if has_permissions == 1 {
            let permissions_length =
                u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
            position += 4;
            let permissions =
                Permissions::from_bytes(&bytes[position..position + permissions_length as usize])?;
            Some(permissions)
        } else {
            None
        };

        let command = UpdatePermissions {
            user_id,
            permissions,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for UpdatePermissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let permissions = if let Some(permissions) = &self.permissions {
            permissions.to_string()
        } else {
            "no_permissions".to_string()
        };
        write!(f, "{}|{}", self.user_id, permissions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::permissions::GlobalPermissions;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = UpdatePermissions {
            user_id: Identifier::numeric(1).unwrap(),
            permissions: Some(get_permissions()),
        };
        let bytes = command.as_bytes();
        let user_id = Identifier::from_bytes(&bytes).unwrap();
        let mut position = user_id.get_size_bytes() as usize;
        let has_permissions = bytes[position];
        position += 1;
        let permissions_length =
            u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        position += 4;
        let permissions =
            Permissions::from_bytes(&bytes[position..position + permissions_length as usize])
                .unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(user_id, command.user_id);
        assert_eq!(has_permissions, 1);
        assert_eq!(permissions, command.permissions.unwrap());
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let user_id = Identifier::numeric(1).unwrap();
        let permissions = get_permissions();
        let permissions_bytes = permissions.as_bytes();
        let mut bytes = Vec::new();
        bytes.extend(user_id.as_bytes());
        bytes.put_u8(1);
        bytes.put_u32_le(permissions_bytes.len() as u32);
        bytes.extend(permissions_bytes);

        let command = UpdatePermissions::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
        assert_eq!(command.permissions.unwrap(), permissions);
    }

    #[test]
    fn should_be_read_from_string() {
        let user_id = Identifier::numeric(1).unwrap();
        let input = user_id.to_string();
        let command = UpdatePermissions::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
    }

    fn get_permissions() -> Permissions {
        Permissions {
            global: GlobalPermissions {
                manage_servers: true,
                read_servers: true,
                manage_users: true,
                read_users: true,
                manage_streams: false,
                read_streams: true,
                manage_topics: false,
                read_topics: true,
                poll_messages: true,
                send_messages: false,
            },
            streams: None,
        }
    }
}
