use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::streams::delete_stream::DeleteStream;
use tracing::info;

#[derive(Debug)]
pub(crate) struct StreamDelete {
    stream_id: Identifier,
}

impl StreamDelete {
    pub(crate) fn new(stream_id: Identifier) -> Self {
        Self { stream_id }
    }
}

#[async_trait]
impl CliCommand for StreamDelete {
    fn explain(&self) -> String {
        format!("delete stream with ID: {}", self.stream_id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        client
            .delete_stream(&DeleteStream {
                stream_id: self.stream_id.clone(),
            })
            .await
            .with_context(|| format!("Problem deleting stream with ID: {}", self.stream_id))?;

        info!("Stream with ID: {} deleted", self.stream_id);

        Ok(())
    }
}
