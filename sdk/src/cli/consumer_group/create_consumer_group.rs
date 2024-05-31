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
        name: String,
        group_id: Option<u32>,
    ) -> Self {
        Self {
            create_consumer_group: CreateConsumerGroup {
                stream_id,
                topic_id,
                name,
                group_id,
            },
        }
    }

    fn get_group_id_info(&self) -> String {
        match self.create_consumer_group.group_id {
            Some(group_id) => format!("ID: {}", group_id),
            None => "ID auto incremented".to_string(),
        }
    }
}

#[async_trait]
impl CliCommand for CreateConsumerGroupCmd {
    fn explain(&self) -> String {
        format!(
            "create consumer group: {}, name: {} for topic with ID: {} and stream with ID: {}",
            self.get_group_id_info(),
            self.create_consumer_group.name,
            self.create_consumer_group.topic_id,
            self.create_consumer_group.stream_id,
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_consumer_group(&self.create_consumer_group.stream_id, &self.create_consumer_group.topic_id, &self.create_consumer_group.name, self.create_consumer_group.group_id)
            .await
            .with_context(|| {
                format!(
                    "Problem creating consumer group ({}, name: {}) for topic with ID: {} and stream with ID: {}",
                    self.get_group_id_info(), self.create_consumer_group.name, self.create_consumer_group.topic_id, self.create_consumer_group.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Consumer group: {}, name: {} created for topic with ID: {} and stream with ID: {}",
            self.get_group_id_info(),
            self.create_consumer_group.name,
            self.create_consumer_group.topic_id,
            self.create_consumer_group.stream_id,
        );

        Ok(())
    }
}
