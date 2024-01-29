use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::messages::append_messages::{AppendMessages, AppendableMessage, Partitioning};
use anyhow::Context;
use async_trait::async_trait;
use std::io::{self, Read};
use std::vec::Vec;
use tracing::{event, Level};

pub struct SendMessagesCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    partitioning: Partitioning,
    messages: Option<Vec<String>>,
}

impl SendMessagesCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
        message_key: Option<String>,
        messages: Option<Vec<String>>,
    ) -> Self {
        let partitioning = match (partition_id, message_key) {
            (Some(_), Some(_)) => unreachable!(),
            (Some(partition_id), None) => Partitioning::partition_id(partition_id),
            (None, Some(message_key)) => Partitioning::messages_key_str(message_key.as_str())
                .unwrap_or_else(|_| {
                    panic!(
                        "Failed to create Partitioning with {} string message key",
                        message_key
                    )
                }),
            (None, None) => Partitioning::default(),
        };
        Self {
            stream_id,
            topic_id,
            partitioning,
            messages,
        }
    }

    fn read_message_from_stdin(&self) -> Result<String, io::Error> {
        let mut buffer = String::new();

        io::stdin().read_to_string(&mut buffer)?;

        Ok(buffer)
    }
}

#[async_trait]
impl CliCommand for SendMessagesCmd {
    fn explain(&self) -> String {
        format!(
            "send messages to topic with ID: {} and stream with ID: {}",
            self.topic_id, self.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let messages = match &self.messages {
            Some(messages) => messages
                .iter()
                .map(|s| AppendableMessage::new(None, s.clone().into(), None))
                .collect::<Vec<_>>(),
            None => {
                let input = self.read_message_from_stdin()?;

                input
                    .lines()
                    .map(|m| AppendableMessage::new(None, String::from(m).into(), None))
                    .collect()
            }
        };

        client
            .send_messages(&mut AppendMessages {
                stream_id: self.stream_id.clone(),
                topic_id: self.topic_id.clone(),
                partitioning: self.partitioning.clone(),
                messages,
            })
            .await
            .with_context(|| {
                format!(
                    "Problem sending messages to topic with ID: {} and stream with ID: {}",
                    self.topic_id, self.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Sent messages to topic with ID: {} and stream with ID: {}",
            self.topic_id,
            self.stream_id,
        );

        Ok(())
    }
}
