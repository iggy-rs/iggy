use crate::args::message_expire::MessageExpiry;
use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::topics::create_topic::CreateTopic;
use tracing::info;

#[derive(Debug)]
pub(crate) struct TopicCreate {
    stream_id: Identifier,
    topic_id: u32,
    partitions_count: u32,
    name: String,
    message_expiry: Option<MessageExpiry>,
}

impl TopicCreate {
    pub(crate) fn new(
        stream_id: Identifier,
        topic_id: u32,
        partitions_count: u32,
        name: String,
        message_expiry: Option<MessageExpiry>,
    ) -> Self {
        Self {
            stream_id,
            topic_id,
            partitions_count,
            name,
            message_expiry,
        }
    }
}

#[async_trait]
impl CliCommand for TopicCreate {
    fn explain(&self) -> String {
        let expiry_text = match &self.message_expiry {
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
                stream_id: self.stream_id.clone(),
                topic_id: self.topic_id,
                partitions_count: self.partitions_count,
                message_expiry: match &self.message_expiry {
                    None => None,
                    Some(value) => value.into(),
                },
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
            match &self.message_expiry {
                Some(value) => format!("message expire time: {}", value),
                None => String::from("without message expire time"),
            },
            self.stream_id,
        );

        Ok(())
    }
}
