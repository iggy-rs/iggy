use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::topics::delete_topic::DeleteTopic;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct DeleteTopicCmd {
    delete_topic: DeleteTopic,
}

impl DeleteTopicCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier) -> Self {
        Self {
            delete_topic: DeleteTopic {
                stream_id,
                topic_id,
            },
        }
    }
}

#[async_trait]
impl CliCommand for DeleteTopicCmd {
    fn explain(&self) -> String {
        format!(
            "delete topic with ID: {} in stream with ID: {}",
            self.delete_topic.topic_id, self.delete_topic.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .delete_topic(&self.delete_topic.stream_id, &self.delete_topic.topic_id)
            .await
            .with_context(|| {
                format!(
                    "Problem deleting topic with ID: {} in stream {}",
                    self.delete_topic.topic_id, self.delete_topic.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with ID: {} in stream with ID: {} deleted",
            self.delete_topic.topic_id, self.delete_topic.stream_id
        );

        Ok(())
    }
}
