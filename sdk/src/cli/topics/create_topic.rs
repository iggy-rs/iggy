use crate::cli::utils::message_expiry::MessageExpiry;
use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::topics::create_topic::CreateTopic;
use crate::utils::byte_size::IggyByteSize;
use anyhow::Context;
use async_trait::async_trait;
use core::fmt;
use tracing::{event, Level};

pub struct CreateTopicCmd {
    create_topic: CreateTopic,
    message_expiry: MessageExpiry,
    max_topic_size: IggyByteSize,
    replication_factor: u8,
}

impl CreateTopicCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: u32,
        partitions_count: u32,
        name: String,
        message_expiry: MessageExpiry,
        max_topic_size: IggyByteSize,
        replication_factor: u8,
    ) -> Self {
        Self {
            create_topic: CreateTopic {
                stream_id,
                topic_id: Some(topic_id),
                partitions_count,
                name,
                message_expiry: message_expiry.clone().into(),
                max_topic_size: Some(max_topic_size),
                replication_factor,
            },
            message_expiry,
            max_topic_size,
            replication_factor,
        }
    }
}

#[async_trait]
impl CliCommand for CreateTopicCmd {
    fn explain(&self) -> String {
        format!("{}", self)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_topic(&self.create_topic)
            .await
            .with_context(|| {
                format!(
                    "Problem creating topic (ID: {}, name: {}, partitions count: {}) in stream with ID: {}",
                    self.create_topic.topic_id.unwrap_or(0), self.create_topic.name, self.create_topic.partitions_count, self.create_topic.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with ID: {}, name: {}, partitions count: {}, message expiry: {}, max topic size: {}, replication factor: {} created in stream with ID: {}",
            self.create_topic.topic_id.unwrap_or(0),
            self.create_topic.name,
            self.create_topic.partitions_count,
            self.message_expiry,
            self.max_topic_size.as_human_string_with_zero_as_unlimited(),
            self.replication_factor,
            self.create_topic.stream_id,
        );

        Ok(())
    }
}

impl fmt::Display for CreateTopicCmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        let topic_id = &self.create_topic.topic_id.unwrap_or(0);
        let topic_name = &self.create_topic.name;
        let message_expiry = &self.message_expiry;
        let max_topic_size = &self.max_topic_size.as_human_string_with_zero_as_unlimited();
        let replication_factor = self.replication_factor;
        let stream_id = &self.create_topic.stream_id;

        write!(
            f,
            "create topic with ID: {topic_id}, name: {topic_name}, message expiry: {message_expiry}, \
            max topic size: {max_topic_size}, replication factor: {replication_factor} in stream with ID: {stream_id}",
        )
    }
}
