use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::consumer::{Consumer, ConsumerKind};
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::identifier::Identifier;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct SetConsumerOffsetCmd {
    set_consumer_offset: StoreConsumerOffset,
}

impl SetConsumerOffsetCmd {
    pub fn new(
        consumer_id: Identifier,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: u32,
        offset: u64,
    ) -> Self {
        Self {
            set_consumer_offset: StoreConsumerOffset {
                consumer: Consumer {
                    kind: ConsumerKind::Consumer,
                    id: consumer_id,
                },
                stream_id,
                topic_id,
                partition_id: Some(partition_id),
                offset,
            },
        }
    }
}

#[async_trait]
impl CliCommand for SetConsumerOffsetCmd {
    fn explain(&self) -> String {
        format!(
            "set consumer offset for consumer with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {} to {}",
            self.set_consumer_offset.consumer.id,
            self.set_consumer_offset.stream_id,
            self.set_consumer_offset.topic_id,
            self.set_consumer_offset.partition_id.unwrap(),
            self.set_consumer_offset.offset,
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .store_consumer_offset(&self.set_consumer_offset.consumer, &self.set_consumer_offset.stream_id, &self.set_consumer_offset.topic_id, self.set_consumer_offset.partition_id, self.set_consumer_offset.offset)
            .await
            .with_context(|| {
                format!(
                    "Problem setting consumer offset for consumer with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {}",
                    self.set_consumer_offset.consumer.id, self.set_consumer_offset.stream_id, self.set_consumer_offset.topic_id, self.set_consumer_offset.partition_id.unwrap()
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Consumer offset for consumer with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {} set to {}",
            self.set_consumer_offset.consumer.id,
            self.set_consumer_offset.stream_id,
            self.set_consumer_offset.topic_id,
            self.set_consumer_offset.partition_id.unwrap(),
            self.set_consumer_offset.offset,
        );

        Ok(())
    }
}
