use crate::cli::utils::message_expiry::MessageExpiry;
use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::topics::create_topic::CreateTopic;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct CreateTopicCmd {
    create_topic: CreateTopic,
    message_expiry: Option<MessageExpiry>,
}

impl CreateTopicCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: u32,
        partitions_count: u32,
        name: String,
        message_expiry: Option<MessageExpiry>,
    ) -> Self {
        Self {
            create_topic: CreateTopic {
                stream_id,
                topic_id,
                partitions_count,
                name,
                message_expiry: match &message_expiry {
                    None => None,
                    Some(value) => value.into(),
                },
            },
            message_expiry,
        }
    }
}

#[async_trait]
impl CliCommand for CreateTopicCmd {
    fn explain(&self) -> String {
        let expiry_text = match &self.message_expiry {
            Some(value) => format!("message expire time: {}", value),
            None => String::from("without message expire time"),
        };
        format!(
            "create topic with ID: {}, name: {}, partitions count: {} and {} in stream with ID: {}",
            self.create_topic.topic_id,
            self.create_topic.name,
            self.create_topic.partitions_count,
            expiry_text,
            self.create_topic.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_topic(&self.create_topic)
            .await
            .with_context(|| {
                format!(
                    "Problem creating topic (ID: {}, name: {}, partitions count: {}) in stream with ID: {}",
                    self.create_topic.topic_id, self.create_topic.name, self.create_topic.partitions_count, self.create_topic.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with ID: {}, name: {}, partitions count: {} and {} created in stream with ID: {}",
            self.create_topic.topic_id,
            self.create_topic.name,
            self.create_topic.partitions_count,
            match &self.message_expiry {
                Some(value) => format!("message expire time: {}", value),
                None => String::from("without message expire time"),
            },
            self.create_topic.stream_id,
        );

        Ok(())
    }
}
