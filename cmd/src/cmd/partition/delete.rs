use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::partitions::delete_partitions::DeletePartitions;
use tracing::info;

#[derive(Debug)]
pub(crate) struct PartitionDelete {
    stream_id: Identifier,
    topic_id: Identifier,
    partitions_count: u32,
}

impl PartitionDelete {
    pub(crate) fn new(stream_id: Identifier, topic_id: Identifier, partitions_count: u32) -> Self {
        Self {
            stream_id,
            topic_id,
            partitions_count,
        }
    }
}

#[async_trait]
impl CliCommand for PartitionDelete {
    fn explain(&self) -> String {
        format!(
            "delete {} partitions for topic with ID: {} and stream with ID: {}",
            self.partitions_count, self.topic_id, self.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        client
            .delete_partitions(&DeletePartitions {
                stream_id: self.stream_id.clone(),
                topic_id: self.topic_id.clone(),
                partitions_count: self.partitions_count,
            })
            .await
            .with_context(|| {
                format!(
                    "Problem deleting {} partitions for topic with ID: {} and stream with ID: {}",
                    self.partitions_count, self.topic_id, self.stream_id
                )
            })?;

        info!(
            "Deleted {} partitions for topic with ID: {} and stream with ID: {}",
            self.partitions_count, self.topic_id, self.stream_id,
        );

        Ok(())
    }
}
