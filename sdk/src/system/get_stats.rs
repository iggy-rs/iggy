use crate::command::{CommandExecution, CommandExecutionOrigin, CommandPayload};
use crate::error::IggyError;
use crate::validatable::Validatable;
use crate::bytes_serializable::BytesSerializable;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `GetStats` command is used to get the statistics about the system.
/// It has no additional payload.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct GetStats {}

impl CommandPayload for GetStats {}
impl CommandExecutionOrigin for GetStats {
    fn get_command_execution_origin(&self) -> CommandExecution {
        CommandExecution::Direct
    }
}

impl Validatable<IggyError> for GetStats {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for GetStats {
    fn as_bytes(&self) -> Bytes {
        Bytes::new()
    }

    fn from_bytes(bytes: Bytes) -> Result<GetStats, IggyError> {
        if !bytes.is_empty() {
            return Err(IggyError::InvalidCommand);
        }

        let command = GetStats {};
        command.validate()?;
        Ok(GetStats {})
    }
}

impl Display for GetStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_empty_bytes() {
        let command = GetStats {};
        let bytes = command.as_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn should_be_deserialized_from_empty_bytes() {
        let command = GetStats::from_bytes(Bytes::new());
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_deserialized_from_empty_bytes() {
        let command = GetStats::from_bytes(Bytes::from_static(&[0]));
        assert!(command.is_err());
    }
}
