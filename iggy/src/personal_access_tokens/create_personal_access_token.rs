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

/// `CreatePersonalAccessToken` command is used to create a new personal access token for the authenticated user.
/// It has additional payload:
/// - `name` - unique name of the token, must be between 3 and 3 characters long.
/// - `expiry` - expiry in seconds (optional), if provided, must be between 1 and 4294967295. Otherwise, the token will never expire.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CreatePersonalAccessToken {
    /// Unique name of the token, must be between 3 and 3 characters long.
    pub name: String,
    /// Expiry in seconds (optional), if provided, must be between 1 and 4294967295. Otherwise, the token will never expire.
    pub expiry: Option<u32>,
}

impl CommandPayload for CreatePersonalAccessToken {}

impl Default for CreatePersonalAccessToken {
    fn default() -> Self {
        CreatePersonalAccessToken {
            name: "token".to_string(),
            expiry: None,
        }
    }
}

impl Validatable<Error> for CreatePersonalAccessToken {
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

impl FromStr for CreatePersonalAccessToken {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.is_empty() || parts.len() > 2 {
            return Err(Error::InvalidCommand);
        }

        let name = parts[0].to_string();
        let expiry = match parts.get(1) {
            Some(expiry) => {
                let expiry = expiry.parse::<u32>()?;
                match expiry {
                    0 => None,
                    _ => Some(expiry),
                }
            }
            None => None,
        };

        let command = CreatePersonalAccessToken { name, expiry };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for CreatePersonalAccessToken {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(5 + self.name.len());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.extend(self.name.as_bytes());
        bytes.put_u32_le(self.expiry.unwrap_or(0));
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<CreatePersonalAccessToken, Error> {
        if bytes.len() < 8 {
            return Err(Error::InvalidCommand);
        }

        let name_length = bytes[0];
        let name = from_utf8(&bytes[1..1 + name_length as usize])?.to_string();
        if name.len() != name_length as usize {
            return Err(Error::InvalidCommand);
        }

        let position = 1 + name_length as usize;
        let expiry = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let expiry = match expiry {
            0 => None,
            _ => Some(expiry),
        };

        let command = CreatePersonalAccessToken { name, expiry };
        command.validate()?;
        Ok(command)
    }
}

impl Display for CreatePersonalAccessToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.name, self.expiry.unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreatePersonalAccessToken {
            name: "test".to_string(),
            expiry: Some(100),
        };

        let bytes = command.as_bytes();
        let name_length = bytes[0];
        let name = from_utf8(&bytes[1..1 + name_length as usize]).unwrap();
        let expiry = u32::from_le_bytes(
            bytes[1 + name_length as usize..5 + name_length as usize]
                .try_into()
                .unwrap(),
        );
        let expiry = match expiry {
            0 => None,
            _ => Some(expiry),
        };

        assert!(!bytes.is_empty());
        assert_eq!(name, command.name);
        assert_eq!(expiry, command.expiry);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let name = "test";
        let expiry = 100;
        let mut bytes = Vec::new();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.extend(name.as_bytes());
        bytes.put_u32_le(expiry);

        let command = CreatePersonalAccessToken::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.name, name);
        assert_eq!(command.expiry, Some(expiry));
    }

    #[test]
    fn should_be_read_from_string() {
        let name = "test";
        let expiry = 100;
        let input = format!("{name}|{expiry}");
        let command = CreatePersonalAccessToken::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.name, name);
        assert_eq!(command.expiry, Some(expiry));
    }
}
