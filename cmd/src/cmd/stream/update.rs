use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::streams::update_stream::UpdateStream;
use tracing::info;

#[derive(Debug)]
pub(crate) struct StreamUpdate {
    id: u32,
    name: String,
}

impl StreamUpdate {
    pub(crate) fn new(id: u32, name: String) -> Self {
        Self { id, name }
    }
}

#[async_trait]
impl CliCommand for StreamUpdate {
    fn explain(&self) -> String {
        format!("update stream with id: {} and name: {}", self.id, self.name)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        client
            .update_stream(&UpdateStream {
                stream_id: Identifier::numeric(self.id).expect("Expected numeric identifier"),
                name: self.name.clone(),
            })
            .await
            .with_context(|| {
                format!(
                    "Problem updating stream with id: {} and name: {}",
                    self.id, self.name
                )
            })?;

        info!("Stream with id: {} updated name: {} ", self.id, self.name);

        Ok(())
    }
}
