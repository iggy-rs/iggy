use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use comfy_table::Table;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::streams::get_stream::GetStream;
use iggy::utils::timestamp::TimeStamp;
use tracing::info;

#[derive(Debug)]
pub(crate) struct StreamGet {
    stream_id: Identifier,
}

impl StreamGet {
    pub(crate) fn new(stream_id: Identifier) -> Self {
        Self { stream_id }
    }
}

#[async_trait]
impl CliCommand for StreamGet {
    fn explain(&self) -> String {
        format!("get stream with ID: {}", self.stream_id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        let stream = client
            .get_stream(&GetStream {
                stream_id: self.stream_id.clone(),
            })
            .await
            .with_context(|| format!("Problem getting stream with ID: {}", self.stream_id))?;

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

        info!("{table}");

        Ok(())
    }
}
