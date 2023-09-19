use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::streams::delete_stream::DeleteStream;
use tracing::info;

#[derive(Debug)]
pub(crate) struct StreamDelete {
    id: u32,
}

impl StreamDelete {
    pub(crate) fn new(id: u32) -> Self {
        Self { id }
    }
}

#[async_trait]
impl CliCommand for StreamDelete {
    fn explain(&self) -> String {
        format!("delete stream with ID: {}", self.id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        client
            .delete_stream(&DeleteStream {
                stream_id: Identifier::numeric(self.id).expect("Expected numeric identifier"),
            })
            .await
            .with_context(|| format!("Problem deleting stream with ID: {}", self.id))?;

        info!("Stream with ID: {} deleted", self.id);

        Ok(())
    }
}
