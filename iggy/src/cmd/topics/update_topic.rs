use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::topics::update_topic::UpdateTopic;
use crate::utils::message_expire::MessageExpiry;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct UpdateTopicCmd {
    update_topic: UpdateTopic,
    message_expiry: Option<MessageExpiry>,
}

impl UpdateTopicCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        name: String,
        message_expiry: Option<MessageExpiry>,
    ) -> Self {
        Self {
            update_topic: UpdateTopic {
                stream_id,
                topic_id,
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
impl CliCommand for UpdateTopicCmd {
    fn explain(&self) -> String {
        let expiry_text = match &self.message_expiry {
            Some(value) => format!(" and message expire time: {}", value),
            None => String::from(""),
        };
        format!(
            "update topic with ID: {}, name: {}{} in stream with ID: {}",
            self.update_topic.topic_id,
            self.update_topic.name,
            expiry_text,
            self.update_topic.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .update_topic(&self.update_topic)
            .await
            .with_context(|| {
                format!(
                    "Problem updating topic (ID: {}, name: {}{}) in stream with ID: {}",
                    self.update_topic.topic_id,
                    self.update_topic.name,
                    match &self.message_expiry {
                        Some(value) => format!(" and message expire time: {}", value),
                        None => String::from(""),
                    },
                    self.update_topic.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with ID: {} updated name: {}{} in stream with ID: {}",
            self.update_topic.topic_id,
            self.update_topic.name,
            match &self.message_expiry {
                Some(value) => format!(" and message expire time: {}", value),
                None => String::from(""),
            },
            self.update_topic.stream_id,
        );

        Ok(())
    }
}
