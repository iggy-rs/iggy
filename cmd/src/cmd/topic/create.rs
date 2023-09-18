use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::topics::create_topic::CreateTopic;
use tracing::info;

#[derive(Debug)]
pub(crate) struct TopicCreate {
    stream_id: u32,
    topic_id: u32,
    partitions_count: u32,
    message_expiry: Option<u32>,
    name: String,
}

impl TopicCreate {
    pub(crate) fn new(
        stream_id: u32,
        topic_id: u32,
        partitions_count: u32,
        message_expiry: Option<u32>,
        name: String,
    ) -> Self {
        Self {
            stream_id,
            topic_id,
            partitions_count,
            message_expiry,
            name,
        }
    }
}

#[async_trait]
impl CliCommand for TopicCreate {
    fn explain(&self) -> String {
        let expiry_text = match self.message_expiry {
            Some(value) => format!("message expire time: {}", value),
            None => String::from("without message expire time"),
        };
        format!(
            "create topic with ID: {}, name: {}, partitions count: {} and {} in stream with ID: {}",
            self.topic_id, self.name, self.partitions_count, expiry_text, self.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        client
            .create_topic(&CreateTopic {
                stream_id: Identifier::numeric(self.stream_id)
                    .expect("Expected numeric identifier"),
                topic_id: self.topic_id,
                partitions_count: self.partitions_count,
                message_expiry: self.message_expiry,
                name: self.name.clone(),
            })
            .await
            .with_context(|| {
                format!(
                    "Problem creating topic (ID: {}, name: {}, partitions count: {}) in stream with ID: {}",
                    self.topic_id, self.name, self.partitions_count, self.stream_id
                )
            })?;

        info!(
            "Topic with ID: {}, name: {}, partitions count: {} and {} created in stream with ID: {}",
            self.topic_id,
            self.name,
            self.partitions_count,
            match self.message_expiry {
                Some(value) => format!("message expire time: {}", value),
                None => String::from("without message expire time"),
            },
            self.stream_id,
        );

        Ok(())
    }
}
