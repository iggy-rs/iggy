use crate::bytes_serializable::BytesSerializable;
use crate::command::{Command, GET_PERSONAL_ACCESS_TOKENS_CODE};
use crate::error::IggyError;
use crate::validatable::Validatable;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `GetPersonalAccessTokens` command is used to get all personal access tokens for the authenticated user.
/// It has no additional payload.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct GetPersonalAccessTokens {}

impl Command for GetPersonalAccessTokens {
    fn code(&self) -> u32 {
        GET_PERSONAL_ACCESS_TOKENS_CODE
    }
}

impl Validatable<IggyError> for GetPersonalAccessTokens {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for GetPersonalAccessTokens {
    fn to_bytes(&self) -> Bytes {
        Bytes::new()
    }

    fn from_bytes(bytes: Bytes) -> Result<GetPersonalAccessTokens, IggyError> {
        if !bytes.is_empty() {
            return Err(IggyError::InvalidCommand);
        }

        Ok(GetPersonalAccessTokens {})
    }
}

impl Display for GetPersonalAccessTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_empty_bytes() {
        let command = GetPersonalAccessTokens {};
        let bytes = command.to_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn should_be_deserialized_from_empty_bytes() {
        let command = GetPersonalAccessTokens::from_bytes(Bytes::new());
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_deserialized_from_empty_bytes() {
        let command = GetPersonalAccessTokens::from_bytes(Bytes::from_static(&[0]));
        assert!(command.is_err());
    }
}
