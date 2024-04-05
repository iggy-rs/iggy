use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::partitions::delete_partitions::DeletePartitions;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct DeletePartitionsCmd {
    delete_partitions: DeletePartitions,
}

impl DeletePartitionsCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, partitions_count: u32) -> Self {
        Self {
            delete_partitions: DeletePartitions {
                stream_id,
                topic_id,
                partitions_count,
            },
        }
    }
}

#[async_trait]
impl CliCommand for DeletePartitionsCmd {
    fn explain(&self) -> String {
        let mut partitions = String::from("partition");
        if self.delete_partitions.partitions_count > 1 {
            partitions.push('s');
        };

        format!(
            "delete {} {partitions} for topic with ID: {} and stream with ID: {}",
            self.delete_partitions.partitions_count,
            self.delete_partitions.topic_id,
            self.delete_partitions.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut partitions = String::from("partition");
        if self.delete_partitions.partitions_count > 1 {
            partitions.push('s');
        };

        client
            .delete_partitions(
                &self.delete_partitions.stream_id,
                &self.delete_partitions.topic_id,
                self.delete_partitions.partitions_count,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem deleting {} {partitions} for topic with ID: {} and stream with ID: {}",
                    self.delete_partitions.partitions_count,
                    self.delete_partitions.topic_id,
                    self.delete_partitions.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Deleted {} {partitions} for topic with ID: {} and stream with ID: {}",
            self.delete_partitions.partitions_count,
            self.delete_partitions.topic_id,
            self.delete_partitions.stream_id,
        );

        Ok(())
    }
}
