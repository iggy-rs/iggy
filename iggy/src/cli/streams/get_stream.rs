use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::streams::get_stream::GetStream;
use crate::utils::timestamp::IggyTimestamp;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{event, Level};

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
            IggyTimestamp::from(stream.created_at)
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
