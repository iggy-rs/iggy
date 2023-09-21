use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::topics::delete_topic::DeleteTopic;
use tracing::info;

#[derive(Debug)]
pub(crate) struct TopicDelete {
    stream_id: u32,
    topic_id: u32,
}

impl TopicDelete {
    pub(crate) fn new(stream_id: u32, topic_id: u32) -> Self {
        Self {
            stream_id,
            topic_id,
        }
    }
}

#[async_trait]
impl CliCommand for TopicDelete {
    fn explain(&self) -> String {
        format!(
            "delete topic with ID: {} in stream with ID: {}",
            self.topic_id, self.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        client
            .delete_topic(&DeleteTopic {
                stream_id: Identifier::numeric(self.stream_id)
                    .expect("Expected numeric identifier for stream_id"),
                topic_id: Identifier::numeric(self.topic_id)
                    .expect("Expected numeric identifier for topic_id"),
            })
            .await
            .with_context(|| {
                format!(
                    "Problem deleting topic with ID: {} in stream {}",
                    self.topic_id, self.stream_id
                )
            })?;

        info!(
            "Topic with ID: {} in stream with ID: {} deleted",
            self.topic_id, self.stream_id
        );

        Ok(())
    }
}
