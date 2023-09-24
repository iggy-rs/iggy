use iggy::cli_command::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::partitions::create_partitions::CreatePartitions;
use tracing::info;

#[derive(Debug)]
pub(crate) struct PartitionCreate {
    stream_id: Identifier,
    topic_id: Identifier,
    partitions_count: u32,
}

impl PartitionCreate {
    pub(crate) fn new(stream_id: Identifier, topic_id: Identifier, partitions_count: u32) -> Self {
        Self {
            stream_id,
            topic_id,
            partitions_count,
        }
    }
}

#[async_trait]
impl CliCommand for PartitionCreate {
    fn explain(&self) -> String {
        format!(
            "create {} partitions for topic with ID: {} and stream with ID: {}",
            self.partitions_count, self.topic_id, self.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        client
            .create_partitions(&CreatePartitions {
                stream_id: self.stream_id.clone(),
                topic_id: self.topic_id.clone(),
                partitions_count: self.partitions_count,
            })
            .await
            .with_context(|| {
                format!(
                    "Problem creating {} partitions for topic with ID: {} and stream with ID: {}",
                    self.partitions_count, self.topic_id, self.stream_id
                )
            })?;

        info!(
            "Created {} partitions for topic with ID: {} and stream with ID: {}",
            self.partitions_count, self.topic_id, self.stream_id,
        );

        Ok(())
    }
}
