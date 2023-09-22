use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::streams::update_stream::UpdateStream;
use tracing::info;

#[derive(Debug)]
pub(crate) struct StreamUpdate {
    stream_id: Identifier,
    name: String,
}

impl StreamUpdate {
    pub(crate) fn new(stream_id: Identifier, name: String) -> Self {
        Self { stream_id, name }
    }
}

#[async_trait]
impl CliCommand for StreamUpdate {
    fn explain(&self) -> String {
        format!(
            "update stream with ID: {} and name: {}",
            self.stream_id, self.name
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        client
            .update_stream(&UpdateStream {
                stream_id: self.stream_id.clone(),
                name: self.name.clone(),
            })
            .await
            .with_context(|| {
                format!(
                    "Problem updating stream with ID: {} and name: {}",
                    self.stream_id, self.name
                )
            })?;

        info!(
            "Stream with ID: {} updated name: {} ",
            self.stream_id, self.name
        );

        Ok(())
    }
}
