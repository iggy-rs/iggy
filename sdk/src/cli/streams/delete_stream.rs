use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::streams::delete_stream::DeleteStream;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

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
            .delete_stream(&self.delete_stream.stream_id)
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
