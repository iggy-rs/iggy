use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `GetClient` command is used to get the information about a specific client by unique ID.
/// It has additional payload:
/// - `client_id` - unique ID (numeric) of the client.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct GetClient {
    /// Unique ID (numeric) of the client.
    pub client_id: u32,
}

impl CommandPayload for GetClient {}

impl Default for GetClient {
    fn default() -> Self {
        GetClient { client_id: 1 }
    }
}

impl Validatable<Error> for GetClient {
    fn validate(&self) -> Result<(), Error> {
        if self.client_id == 0 {
            return Err(Error::InvalidClientId);
        }

        Ok(())
    }
}

impl FromStr for GetClient {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 1 {
            return Err(Error::InvalidCommand);
        }

        let client_id = parts[0].parse::<u32>()?;
        let command = GetClient { client_id };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for GetClient {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4);
        bytes.put_u32_le(self.client_id);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<GetClient, Error> {
        if bytes.len() != 4 {
            return Err(Error::InvalidCommand);
        }

        let client_id = u32::from_le_bytes(bytes.try_into()?);
        let command = GetClient { client_id };
        command.validate()?;
        Ok(command)
    }
}

impl Display for GetClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.client_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = GetClient { client_id: 1 };

        let bytes = command.as_bytes();
        let client_id = u32::from_le_bytes(bytes[..4].try_into().unwrap());

        assert!(!bytes.is_empty());
        assert_eq!(client_id, command.client_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let client_id = 1u32;
        let bytes = client_id.to_le_bytes();
        let command = GetClient::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.client_id, client_id);
    }

    #[test]
    fn should_be_read_from_string() {
        let client_id = 1u32;
        let input = client_id.to_string();
        let command = GetClient::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.client_id, client_id);
    }
}
