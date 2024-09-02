use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use anyhow::{Context, Error};
use async_trait::async_trait;
use tracing::{event, Level};

pub struct FlushMessagesCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: u32,
    fsync: bool,
}

impl FlushMessagesCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Self {
        Self {
            stream_id,
            topic_id,
            partition_id,
            fsync,
        }
    }
}

#[async_trait]
impl CliCommand for FlushMessagesCmd {
    fn explain(&self) -> String {
        format!(
            "flush messages from topic with ID: {} and stream with ID: {} (partition with ID: {}) {}",
            self.topic_id,
            self.stream_id,
            self.partition_id,
            if self.fsync {
                "with fsync"
            } else {
                "without fsync"
            },
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), Error> {
        client
            .flush_unsaved_buffer(
                &self.stream_id,
                &self.topic_id,
                self.partition_id,
                self.fsync,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem flushing messages from topic with ID: {} and stream with ID: {} (partition with ID: {}) {}",
                    self.topic_id, self.stream_id, self.partition_id, if self.fsync { "with fsync" } else { "without fsync" },
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Flushed messages from topic with ID: {} and stream with ID: {} (partition with ID: {}) {}",
            self.topic_id,
            self.stream_id,
            self.partition_id,
            if self.fsync { "with fsync" } else { "without fsync" },
        );

        Ok(())
    }
}
