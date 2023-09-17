use crate::cli::CliCommand;
// use crate::error::IggyConsoleError;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use comfy_table::Table;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::streams::get_stream::GetStream;
use iggy::utils::timestamp::TimeStamp;

#[derive(Debug)]
pub(crate) struct StreamGet {
    id: u32,
}

impl StreamGet {
    pub(crate) fn new(id: u32) -> Self {
        Self { id }
    }
}

#[async_trait]
impl CliCommand for StreamGet {
    fn explain(&self) -> String {
        format!("get stream {}", self.id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        let stream = client
            .get_stream(&GetStream {
                stream_id: Identifier::numeric(self.id).expect("Expected numeric identifier"),
            })
            .await
            .with_context(|| format!("Problem getting stream (id: {})", self.id))?;

        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec!["Stream id", format!("{}", stream.id).as_str()]);
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

        println!("{table}");

        Ok(())
    }
}
