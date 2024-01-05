use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::identifier::Identifier;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct CreateConsumerGroupCmd {
    create_consumer_group: CreateConsumerGroup,
}

impl CreateConsumerGroupCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        consumer_group_id: u32,
        name: String,
    ) -> Self {
        Self {
            create_consumer_group: CreateConsumerGroup {
                stream_id,
                topic_id,
                consumer_group_id,
                name,
            },
        }
    }
}

#[async_trait]
impl CliCommand for CreateConsumerGroupCmd {
    fn explain(&self) -> String {
        format!(
            "create consumer group with ID: {}, name: {} for topic with ID: {} and stream with ID: {}",
            self.create_consumer_group.consumer_group_id,
            self.create_consumer_group.name,
            self.create_consumer_group.topic_id,
            self.create_consumer_group.stream_id,
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_consumer_group(&self.create_consumer_group)
            .await
            .with_context(|| {
                format!(
                    "Problem creating consumer group (ID: {}, name: {}) for topic with ID: {} and stream with ID: {}",
                    self.create_consumer_group.consumer_group_id, self.create_consumer_group.name, self.create_consumer_group.topic_id, self.create_consumer_group.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Consumer group with ID: {}, name: {} created for topic with ID: {} and stream with ID: {}",
            self.create_consumer_group.consumer_group_id,
            self.create_consumer_group.name,
            self.create_consumer_group.topic_id,
            self.create_consumer_group.stream_id,
        );

        Ok(())
    }
}
