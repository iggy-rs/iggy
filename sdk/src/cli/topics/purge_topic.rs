use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::topics::purge_topic::PurgeTopic;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct PurgeTopicCmd {
    purge_topic: PurgeTopic,
}

impl PurgeTopicCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier) -> Self {
        Self {
            purge_topic: PurgeTopic {
                stream_id,
                topic_id,
            },
        }
    }
}

#[async_trait]
impl CliCommand for PurgeTopicCmd {
    fn explain(&self) -> String {
        format!(
            "purge topic with ID: {} in stream with ID: {}",
            self.purge_topic.topic_id, self.purge_topic.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .purge_topic(&self.purge_topic.stream_id, &self.purge_topic.topic_id)
            .await
            .with_context(|| {
                format!(
                    "Problem purging topic with ID: {} in stream {}",
                    self.purge_topic.topic_id, self.purge_topic.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with ID: {} in stream with ID: {} purged",
            self.purge_topic.topic_id, self.purge_topic.stream_id);

        Ok(())
    }
}
