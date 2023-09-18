use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::topics::update_topic::UpdateTopic;
use tracing::info;

#[derive(Debug)]
pub(crate) struct TopicUpdate {
    stream_id: u32,
    topic_id: u32,
    name: String,
    message_expiry: Option<u32>,
}

impl TopicUpdate {
    pub(crate) fn new(
        stream_id: u32,
        topic_id: u32,
        name: String,
        message_expiry: Option<u32>,
    ) -> Self {
        Self {
            stream_id,
            topic_id,
            name,
            message_expiry,
        }
    }
}

#[async_trait]
impl CliCommand for TopicUpdate {
    fn explain(&self) -> String {
        let expiry_text = match self.message_expiry {
            Some(value) => format!(" and message expire time: {}", value),
            None => String::from(""),
        };
        format!(
            "update topic with ID: {}, name: {}{} in stream with ID: {}",
            self.topic_id, self.name, expiry_text, self.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        client
            .update_topic(&UpdateTopic {
                stream_id: Identifier::numeric(self.stream_id)
                    .expect("Expected numeric identifier for stream ID"),
                topic_id: Identifier::numeric(self.topic_id)
                    .expect("Expected numeric identifier for topic ID"),
                message_expiry: self.message_expiry,
                name: self.name.clone(),
            })
            .await
            .with_context(|| {
                format!(
                    "Problem updating topic (ID: {}, name: {}{}) in stream with ID: {}",
                    self.topic_id,
                    self.name,
                    match self.message_expiry {
                        Some(value) => format!(" and message expire time: {}", value),
                        None => String::from(""),
                    },
                    self.stream_id
                )
            })?;

        info!(
            "Topic with ID: {} updated name: {}{} in stream with ID: {}",
            self.topic_id,
            self.name,
            match self.message_expiry {
                Some(value) => format!(" and message expire time: {}", value),
                None => String::from(""),
            },
            self.stream_id,
        );

        Ok(())
    }
}
