use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::streams::create_stream::CreateStream;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct CreateStreamCmd {
    create_stream: CreateStream,
}

impl CreateStreamCmd {
    pub fn new(stream_id: u32, name: String) -> Self {
        Self {
            create_stream: CreateStream { stream_id, name },
        }
    }
}

#[async_trait]
impl CliCommand for CreateStreamCmd {
    fn explain(&self) -> String {
        format!(
            "create stream with ID: {} and name: {}",
            self.create_stream.stream_id, self.create_stream.name
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_stream(&self.create_stream)
            .await
            .with_context(|| {
                format!(
                    "Problem creating stream (ID: {} and name: {})",
                    self.create_stream.stream_id, self.create_stream.name
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Stream with ID: {} and name: {} created",
            self.create_stream.stream_id, self.create_stream.name
        );

        Ok(())
    }
}
