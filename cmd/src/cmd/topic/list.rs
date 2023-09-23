use crate::args::common::ListMode;
use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use comfy_table::Table;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::topics::get_topics::GetTopics;
use iggy::utils::timestamp::TimeStamp;
use tracing::info;

#[derive(Debug)]
pub(crate) struct TopicList {
    stream_id: Identifier,
    list_mode: ListMode,
}

impl TopicList {
    pub(crate) fn new(stream_id: Identifier, list_mode: ListMode) -> Self {
        Self {
            stream_id,
            list_mode,
        }
    }
}

#[async_trait]
impl CliCommand for TopicList {
    fn explain(&self) -> String {
        format!("get topics from stream with ID: {}", self.stream_id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        let topics = client
            .get_topics(&GetTopics {
                stream_id: self.stream_id.clone(),
            })
            .await
            .with_context(|| format!("Problem getting topics from stream {}", self.stream_id))?;

        match self.list_mode {
            ListMode::Table => {
                let mut table = Table::new();

                table.set_header(vec![
                    "ID",
                    "Created",
                    "Name",
                    "Size (B)",
                    "Message Expiry (s)",
                    "Messages Count",
                    "Partitions Count",
                ]);

                topics.iter().for_each(|topic| {
                    table.add_row(vec![
                        format!("{}", topic.id),
                        TimeStamp::from(topic.created_at).to_string("%Y-%m-%d %H:%M:%S"),
                        topic.name.clone(),
                        format!("{}", topic.size_bytes),
                        match topic.message_expiry {
                            Some(value) => format!("{}", value),
                            None => String::from("None"),
                        },
                        format!("{}", topic.messages_count),
                        format!("{}", topic.partitions_count),
                    ]);
                });

                info!("{table}");
            }
            ListMode::List => {
                topics.iter().for_each(|topic| {
                    info!(
                        "{}|{}|{}|{}|{}|{}|{}",
                        topic.id,
                        TimeStamp::from(topic.created_at).to_string("%Y-%m-%d %H:%M:%S"),
                        topic.name,
                        topic.size_bytes,
                        match topic.message_expiry {
                            Some(value) => format!("{}", value),
                            None => String::from("None"),
                        },
                        topic.messages_count,
                        topic.partitions_count
                    );
                });
            }
        }

        Ok(())
    }
}
