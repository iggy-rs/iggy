use crate::bytes_serializable::BytesSerializable;
use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::utils::timestamp::TimeStamp;
use crate::validatable::Validatable;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use tracing::{event, Level};

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct GetStream {
    #[serde(skip)]
    pub stream_id: Identifier,
}

impl CommandPayload for GetStream {}

impl Validatable<Error> for GetStream {
    fn validate(&self) -> std::result::Result<(), Error> {
        Ok(())
    }
}

impl FromStr for GetStream {
    type Err = Error;
    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 1 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let command = GetStream { stream_id };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for GetStream {
    fn as_bytes(&self) -> Vec<u8> {
        self.stream_id.as_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<GetStream, Error> {
        if bytes.len() < 3 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = Identifier::from_bytes(bytes)?;
        let command = GetStream { stream_id };
        command.validate()?;
        Ok(command)
    }
}

impl Display for GetStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.stream_id)
    }
}

pub struct GetStreamCmd {
    get_stream: GetStream,
}

impl GetStreamCmd {
    pub fn new(stream_id: Identifier) -> Self {
        Self {
            get_stream: GetStream { stream_id },
        }
    }
}

#[async_trait]
impl CliCommand for GetStreamCmd {
    fn explain(&self) -> String {
        format!("get stream with ID: {}", self.get_stream.stream_id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let stream = client.get_stream(&self.get_stream).await.with_context(|| {
            format!(
                "Problem getting stream with ID: {}",
                self.get_stream.stream_id
            )
        })?;

        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec!["Stream ID", format!("{}", stream.id).as_str()]);
        table.add_row(vec![
            "Created",
            TimeStamp::from(stream.created_at)
                .to_string("%Y-%m-%d %H:%M:%S")
                .as_str(),
        ]);
        table.add_row(vec!["Stream name", stream.name.as_str()]);
        table.add_row(vec![
            "Stream size",
            format!("{}", stream.size_bytes).as_str(),
        ]);
        table.add_row(vec![
            "Stream message count",
            format!("{}", stream.messages_count).as_str(),
        ]);
        table.add_row(vec![
            "Stream topics count",
            format!("{}", stream.topics_count).as_str(),
        ]);

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = GetStream {
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
        let command = GetStream::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let input = stream_id.to_string();
        let command = GetStream::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
    }
}
