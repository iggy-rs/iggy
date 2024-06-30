use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::consumer::Consumer;
use crate::identifier::Identifier;
use crate::messages::poll_messages::{PollMessages, PollingStrategy};
use crate::models::header::{HeaderKey, HeaderKind};
use crate::models::messages::PolledMessages;
use crate::utils::timestamp::IggyTimestamp;
use crate::utils::{byte_size::IggyByteSize, duration::IggyDuration};
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::{Cell, CellAlignment, Row, Table};
use std::collections::HashSet;
use std::mem::size_of_val;
use tracing::{event, Level};

pub struct PollMessagesCmd {
    poll_messages: PollMessages,
    show_headers: bool,
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
        show_headers: bool,
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
            show_headers,
        }
    }

    fn create_message_header_keys(
        &self,
        polled_messages: &PolledMessages,
    ) -> HashSet<(HeaderKey, HeaderKind)> {
        if !self.show_headers {
            return HashSet::new();
        }
        polled_messages
            .messages
            .iter()
            .flat_map(|m| match m.headers.as_ref() {
                Some(h) => h
                    .iter()
                    .map(|(k, v)| (k.clone(), v.kind))
                    .collect::<Vec<_>>(),
                None => vec![],
            })
            .collect::<HashSet<_>>()
    }

    fn create_table_header(header_key_set: &HashSet<(HeaderKey, HeaderKind)>) -> Row {
        let mut table_header = vec![
            Cell::new("Offset"),
            Cell::new("Timestamp"),
            Cell::new("ID"),
            Cell::new("Length"),
            Cell::new("Payload"),
        ];
        let message_headers = header_key_set
            .iter()
            .map(|(key, kind)| {
                Cell::new(format!("Header: {}\n{}", key.as_str(), kind))
                    .set_alignment(CellAlignment::Center)
            })
            .collect::<Vec<_>>();
        table_header.extend(message_headers);
        Row::from(table_header)
    }

    fn create_table_content(
        polled_messages: &PolledMessages,
        message_header_keys: &HashSet<(HeaderKey, HeaderKind)>,
    ) -> Vec<Row> {
        polled_messages
            .messages
            .iter()
            .map(|message| {
                let mut row = vec![
                    format!("{}", message.offset),
                    IggyTimestamp::from(message.timestamp).to_local_string("%Y-%m-%d %H:%M:%S%.6f"),
                    format!("{}", message.id),
                    format!("{}", message.payload.len()),
                    String::from_utf8_lossy(&message.payload).to_string(),
                ];

                let values = message_header_keys
                    .iter()
                    .map(|(key, kind)| {
                        message
                            .headers
                            .as_ref()
                            .map(|h| {
                                h.get(key)
                                    .filter(|v| v.kind == *kind)
                                    .map(|v| v.value_only_to_string())
                                    .unwrap_or_default()
                            })
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>();
                row.extend(values);
                Row::from(row)
            })
            .collect::<_>()
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

        let message_header_keys = self.create_message_header_keys(&messages);

        let mut table = Table::new();
        let table_header = Self::create_table_header(&message_header_keys);
        let table_content = Self::create_table_content(&messages, &message_header_keys);
        table.set_header(table_header);
        table.add_rows(table_content);

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
