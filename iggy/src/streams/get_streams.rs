use crate::bytes_serializable::BytesSerializable;
use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::utils::timestamp::TimeStamp;
use crate::validatable::Validatable;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use tracing::{event, Level};

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct GetStreams {}

impl CommandPayload for GetStreams {}

impl Validatable<Error> for GetStreams {
    fn validate(&self) -> std::result::Result<(), Error> {
        Ok(())
    }
}

impl FromStr for GetStreams {
    type Err = Error;
    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        if !input.is_empty() {
            return Err(Error::InvalidCommand);
        }

        let command = GetStreams {};
        command.validate()?;
        Ok(GetStreams {})
    }
}

impl BytesSerializable for GetStreams {
    fn as_bytes(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<GetStreams, Error> {
        if !bytes.is_empty() {
            return Err(Error::InvalidCommand);
        }

        let command = GetStreams {};
        command.validate()?;
        Ok(GetStreams {})
    }
}

impl Display for GetStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

pub enum GetStreamsOutput {
    Table,
    List,
}

pub struct GetStreamsCmd {
    get_streams: GetStreams,
    output: GetStreamsOutput,
}

impl GetStreamsCmd {
    pub fn new(output: GetStreamsOutput) -> Self {
        GetStreamsCmd {
            get_streams: GetStreams {},
            output,
        }
    }
}

impl Default for GetStreamsCmd {
    fn default() -> Self {
        GetStreamsCmd {
            get_streams: GetStreams {},
            output: GetStreamsOutput::Table,
        }
    }
}

#[async_trait]
impl CliCommand for GetStreamsCmd {
    fn explain(&self) -> String {
        let mode = match self.output {
            GetStreamsOutput::Table => "table",
            GetStreamsOutput::List => "list",
        };
        format!("list streams in {mode} mode")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let streams = client
            .get_streams(&self.get_streams)
            .await
            .with_context(|| String::from("Problem getting list of streams"))?;

        if streams.is_empty() {
            event!(target: PRINT_TARGET, Level::INFO, "No streams found!");
            return Ok(());
        }

        match self.output {
            GetStreamsOutput::Table => {
                let mut table = Table::new();

                table.set_header(vec![
                    "ID", "Created", "Name", "Size (B)", "Messages", "Topics",
                ]);

                streams.iter().for_each(|stream| {
                    table.add_row(vec![
                        format!("{}", stream.id),
                        TimeStamp::from(stream.created_at).to_string("%Y-%m-%d %H:%M:%S"),
                        stream.name.clone(),
                        format!("{}", stream.size_bytes),
                        format!("{}", stream.messages_count),
                        format!("{}", stream.topics_count),
                    ]);
                });

                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetStreamsOutput::List => {
                streams.iter().for_each(|stream| {
                    event!(target: PRINT_TARGET, Level::INFO,
                        "{}|{}|{}|{}|{}|{}",
                        stream.id,
                        TimeStamp::from(stream.created_at).to_string("%Y-%m-%d %H:%M:%S"),
                        stream.name,
                        stream.size_bytes,
                        stream.messages_count,
                        stream.topics_count
                    );
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_empty_bytes() {
        let command = GetStreams {};
        let bytes = command.as_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn should_be_deserialized_from_empty_bytes() {
        let bytes: Vec<u8> = vec![];
        let command = GetStreams::from_bytes(&bytes);
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_deserialized_from_empty_bytes() {
        let bytes: Vec<u8> = vec![0];
        let command = GetStreams::from_bytes(&bytes);
        assert!(command.is_err());
    }

    #[test]
    fn should_be_read_from_empty_string() {
        let input = "";
        let command = GetStreams::from_str(input);
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_read_from_non_empty_string() {
        let input = " ";
        let command = GetStreams::from_str(input);
        assert!(command.is_err());
    }
}
