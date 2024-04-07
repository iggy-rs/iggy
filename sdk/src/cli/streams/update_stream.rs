use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::streams::update_stream::UpdateStream;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct UpdateStreamCmd {
    update_stream: UpdateStream,
}

impl UpdateStreamCmd {
    pub fn new(stream_id: Identifier, name: String) -> Self {
        UpdateStreamCmd {
            update_stream: UpdateStream { stream_id, name },
        }
    }
}

#[async_trait]
impl CliCommand for UpdateStreamCmd {
    fn explain(&self) -> String {
        format!(
            "update stream with ID: {} and name: {}",
            self.update_stream.stream_id, self.update_stream.name
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .update_stream(&self.update_stream.stream_id, &self.update_stream.name)
            .await
            .with_context(|| {
                format!(
                    "Problem updating stream with ID: {} and name: {}",
                    self.update_stream.stream_id, self.update_stream.name
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Stream with ID: {} updated name: {}",
            self.update_stream.stream_id, self.update_stream.name
        );

        Ok(())
    }
}
