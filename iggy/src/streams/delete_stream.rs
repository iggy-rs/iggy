use crate::cli_command::CliCommand;
use crate::client::Client;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use crate::{bytes_serializable::BytesSerializable, cli_command::PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use tracing::{event, Level};

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct DeleteStream {
    #[serde(skip)]
    pub stream_id: Identifier,
}

impl CommandPayload for DeleteStream {}

impl Validatable<Error> for DeleteStream {
    fn validate(&self) -> std::result::Result<(), Error> {
        Ok(())
    }
}

impl FromStr for DeleteStream {
    type Err = Error;
    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 1 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let command = DeleteStream { stream_id };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for DeleteStream {
    fn as_bytes(&self) -> Vec<u8> {
        self.stream_id.as_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<DeleteStream, Error> {
        if bytes.len() < 3 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = Identifier::from_bytes(bytes)?;
        let command = DeleteStream { stream_id };
        command.validate()?;
        Ok(command)
    }
}

impl Display for DeleteStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.stream_id)
    }
}

pub struct DeleteStreamCmd {
    delete_stream: DeleteStream,
}

impl DeleteStreamCmd {
    pub fn new(stream_id: Identifier) -> Self {
        Self {
            delete_stream: DeleteStream { stream_id },
        }
    }
}

#[async_trait]
impl CliCommand for DeleteStreamCmd {
    fn explain(&self) -> String {
        format!("delete stream with ID: {}", self.delete_stream.stream_id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .delete_stream(&self.delete_stream)
            .await
            .with_context(|| {
                format!(
                    "Problem deleting stream with ID: {}",
                    self.delete_stream.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO, "Stream with ID: {} deleted", self.delete_stream.stream_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = DeleteStream {
            stream_id: Identifier::numeric(1).unwrap(),
        };

        let bytes = command.as_bytes();
        let stream_id = Identifier::from_bytes(&bytes).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let bytes = stream_id.as_bytes();
        let command = DeleteStream::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let input = stream_id.to_string();
        let command = DeleteStream::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }
}
