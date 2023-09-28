use crate::bytes_serializable::BytesSerializable;
use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::utils::text;
use crate::validatable::Validatable;
use anyhow::Context;
use async_trait::async_trait;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::{from_utf8, FromStr};
use tracing::{event, Level};

const MAX_NAME_LENGTH: usize = 255;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateStream {
    pub stream_id: u32,
    pub name: String,
}

impl CommandPayload for CreateStream {}

impl Default for CreateStream {
    fn default() -> Self {
        CreateStream {
            stream_id: 1,
            name: "stream".to_string(),
        }
    }
}

impl Validatable<Error> for CreateStream {
    fn validate(&self) -> std::result::Result<(), Error> {
        if self.stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(Error::InvalidStreamName);
        }

        if !text::is_resource_name_valid(&self.name) {
            return Err(Error::InvalidStreamName);
        }

        Ok(())
    }
}

impl FromStr for CreateStream {
    type Err = Error;
    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<u32>()?;
        let name = parts[1].to_string();
        let command = CreateStream { stream_id, name };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for CreateStream {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(5 + self.name.len());
        bytes.put_u32_le(self.stream_id);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.extend(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<CreateStream, Error> {
        if bytes.len() < 6 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes[..4].try_into()?);
        let name_length = bytes[4];
        let name = from_utf8(&bytes[5..5 + name_length as usize])?.to_string();
        if name.len() != name_length as usize {
            return Err(Error::InvalidCommand);
        }

        let command = CreateStream { stream_id, name };
        command.validate()?;
        Ok(command)
    }
}

impl Display for CreateStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.stream_id, self.name)
    }
}

pub struct CreateStreamCmd {
    create_stream: CreateStream,
}

impl CreateStreamCmd {
    pub fn new(stream_id: u32, name: String) -> Self {
        Self {
            create_stream: CreateStream { stream_id, name },
        }
    }
}

#[async_trait]
impl CliCommand for CreateStreamCmd {
    fn explain(&self) -> String {
        format!(
            "create stream with ID: {} and name: {}",
            self.create_stream.stream_id, self.create_stream.name
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_stream(&self.create_stream)
            .await
            .with_context(|| {
                format!(
                    "Problem creating stream (ID: {} and name: {})",
                    self.create_stream.stream_id, self.create_stream.name
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Stream with ID: {} and name: {} created",
            self.create_stream.stream_id, self.create_stream.name
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreateStream {
            stream_id: 1,
            name: "test".to_string(),
        };

        let bytes = command.as_bytes();
        let stream_id = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        let name_length = bytes[4];
        let name = from_utf8(&bytes[5..5 + name_length as usize]).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = 1u32;
        let name = "test".to_string();
        let mut bytes = Vec::new();
        bytes.put_u32_le(stream_id);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.extend(name.as_bytes());
        let command = CreateStream::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.name, name);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = 1u32;
        let name = "test".to_string();
        let input = format!("{}|{}", stream_id, name);
        let command = CreateStream::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.name, name);
    }
}
