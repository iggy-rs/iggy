use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::streams::get_streams::GetStreams;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{event, Level};

pub enum GetStreamsOutput {
    Table,
    List,
}

pub struct GetStreamsCmd {
    _get_streams: GetStreams,
    output: GetStreamsOutput,
}

impl GetStreamsCmd {
    pub fn new(output: GetStreamsOutput) -> Self {
        GetStreamsCmd {
            _get_streams: GetStreams {},
            output,
        }
    }
}

impl Default for GetStreamsCmd {
    fn default() -> Self {
        GetStreamsCmd {
            _get_streams: GetStreams {},
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
            .get_streams()
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
                        format!("{}", stream.created_at),
                        stream.name.clone(),
                        format!("{}", stream.size),
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
                        stream.created_at,
                        stream.name,
                        stream.size,
                        stream.messages_count,
                        stream.topics_count
                    );
                });
            }
        }

        Ok(())
    }
}
