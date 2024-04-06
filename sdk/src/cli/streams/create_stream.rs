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
    pub fn new(stream_id: Option<u32>, name: String) -> Self {
        Self {
            create_stream: CreateStream { stream_id, name },
        }
    }

    fn get_stream_id_info(&self) -> String {
        match self.create_stream.stream_id {
            Some(stream_id) => format!("ID: {}", stream_id),
            None => "ID auto incremented".to_string(),
        }
    }
}

#[async_trait]
impl CliCommand for CreateStreamCmd {
    fn explain(&self) -> String {
        format!(
            "create stream with name: {} and {}",
            self.create_stream.name,
            self.get_stream_id_info(),
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_stream(&self.create_stream.name, self.create_stream.stream_id)
            .await
            .with_context(|| {
                format!(
                    "Problem creating stream (name: {} and {})",
                    self.create_stream.name,
                    self.get_stream_id_info(),
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Stream with name: {} and {} created",
            self.create_stream.name,
            self.get_stream_id_info(),
        );

        Ok(())
    }
}
