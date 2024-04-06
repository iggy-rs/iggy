use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::streams::purge_stream::PurgeStream;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct PurgeStreamCmd {
    purge_stream: PurgeStream,
}

impl PurgeStreamCmd {
    pub fn new(stream_id: Identifier) -> Self {
        Self {
            purge_stream: PurgeStream { stream_id },
        }
    }
}

#[async_trait]
impl CliCommand for PurgeStreamCmd {
    fn explain(&self) -> String {
        format!("purge stream with ID: {}", self.purge_stream.stream_id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .purge_stream(&self.purge_stream.stream_id)
            .await
            .with_context(|| {
                format!(
                    "Problem purging stream with ID: {}",
                    self.purge_stream.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO, "Stream with ID: {} purged", self.purge_stream.stream_id);

        Ok(())
    }
}
