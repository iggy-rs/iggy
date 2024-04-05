use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::consumer::Consumer;
use crate::identifier::Identifier;
use crate::messages::poll_messages::{PollMessages, PollingStrategy};
use crate::utils::{byte_size::IggyByteSize, duration::IggyDuration, timestamp::IggyTimestamp};
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use std::mem::size_of_val;
use tracing::{event, Level};

pub struct PollMessagesCmd {
    poll_messages: PollMessages,
}

impl PollMessagesCmd {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: u32,
        message_count: u32,
        auto_commit: bool,
        offset: Option<u64>,
        first: bool,
        last: bool,
        next: bool,
        consumer: Identifier,
    ) -> Self {
        let strategy = match (offset, first, last, next) {
            (Some(offset), false, false, false) => PollingStrategy::offset(offset),
            (None, true, false, false) => PollingStrategy::first(),
            (None, false, true, false) => PollingStrategy::last(),
            (None, false, false, true) => PollingStrategy::next(),
            _ => unreachable!("Either offset or first, last or next must be specified"),
        };
        Self {
            poll_messages: PollMessages {
                consumer: Consumer::new(consumer),
                stream_id,
                topic_id,
                partition_id: Some(partition_id),
                strategy,
                count: message_count,
                auto_commit,
            },
        }
    }
}

#[async_trait]
impl CliCommand for PollMessagesCmd {
    fn explain(&self) -> String {
        format!(
            "poll messages from topic ID: {} and stream with ID: {}",
            self.poll_messages.topic_id, self.poll_messages.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let start = std::time::Instant::now();
        let messages = client
            .poll_messages(
                &self.poll_messages.stream_id,
                &self.poll_messages.topic_id,
                self.poll_messages.partition_id,
                &self.poll_messages.consumer,
                &self.poll_messages.strategy,
                self.poll_messages.count,
                self.poll_messages.auto_commit,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem polling messages to topic with ID: {} and stream with ID: {}",
                    self.poll_messages.topic_id, self.poll_messages.stream_id
                )
            })?;
        let elapsed = IggyDuration::new(start.elapsed());

        event!(target: PRINT_TARGET, Level::INFO,
            "Polled messages from topic with ID: {} and stream with ID: {} (from partition with ID: {})",
            self.poll_messages.topic_id,
            self.poll_messages.stream_id,
            messages.partition_id,
        );

        let polled_size = IggyByteSize::from(
            messages
                .messages
                .iter()
                .map(|m| size_of_val(m) as u64)
                .sum::<u64>(),
        );

        event!(target: PRINT_TARGET, Level::INFO, "Polled {} messages of total size {polled_size}, it took {}", messages.messages.len(), elapsed.as_human_time_string());

        let mut table = Table::new();
        table.set_header(vec!["Offset", "Timestamp", "ID", "Length", "Payload"]);

        messages.messages.iter().for_each(|message| {
            table.add_row(vec![
                format!("{}", message.offset),
                IggyTimestamp::from(message.timestamp).to_local_string("%Y-%m-%d %H:%M:%S%.6f"),
                format!("{}", message.id),
                format!("{}", message.payload.len()),
                String::from_utf8_lossy(&message.payload).to_string(),
            ]);
        });

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
