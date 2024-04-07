use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::partitions::create_partitions::CreatePartitions;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct CreatePartitionsCmd {
    create_partition: CreatePartitions,
}

impl CreatePartitionsCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, partitions_count: u32) -> Self {
        Self {
            create_partition: CreatePartitions {
                stream_id,
                topic_id,
                partitions_count,
            },
        }
    }
}

#[async_trait]
impl CliCommand for CreatePartitionsCmd {
    fn explain(&self) -> String {
        let mut partitions = String::from("partition");
        if self.create_partition.partitions_count > 1 {
            partitions.push('s');
        };

        format!(
            "create {} {partitions} for topic with ID: {} and stream with ID: {}",
            self.create_partition.partitions_count,
            self.create_partition.topic_id,
            self.create_partition.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut partitions = String::from("partition");
        if self.create_partition.partitions_count > 1 {
            partitions.push('s');
        };

        client
            .create_partitions(
                &self.create_partition.stream_id,
                &self.create_partition.topic_id,
                self.create_partition.partitions_count,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem creating {} {partitions} for topic with ID: {} and stream with ID: {}",
                    self.create_partition.partitions_count,
                    self.create_partition.topic_id,
                    self.create_partition.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Created {} {partitions} for topic with ID: {} and stream with ID: {}",
            self.create_partition.partitions_count,
            self.create_partition.topic_id,
            self.create_partition.stream_id,
        );

        Ok(())
    }
}
