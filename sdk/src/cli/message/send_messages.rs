use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::messages::send_messages::{Message, Partitioning};
use crate::models::header::{HeaderKey, HeaderValue};
use anyhow::Context;
use async_trait::async_trait;
use std::collections::HashMap;
use std::io::{self, Read};
use tracing::{event, Level};

pub struct SendMessagesCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    partitioning: Partitioning,
    messages: Option<Vec<String>>,
    headers: Vec<(HeaderKey, HeaderValue)>,
}

impl SendMessagesCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
        message_key: Option<String>,
        messages: Option<Vec<String>>,
        headers: Vec<(HeaderKey, HeaderValue)>,
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
            headers,
        }
    }

    fn read_message_from_stdin(&self) -> Result<String, io::Error> {
        let mut buffer = String::new();

        io::stdin().read_to_string(&mut buffer)?;

        Ok(buffer)
    }

    fn get_headers(&self) -> Option<HashMap<HeaderKey, HeaderValue>> {
        match self.headers.len() {
            0 => None,
            _ => Some(self.headers.iter().cloned().collect()),
        }
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
        let mut messages = match &self.messages {
            Some(messages) => messages
                .iter()
                .map(|s| Message::new(None, s.clone().into(), self.get_headers()))
                .collect::<Vec<_>>(),
            None => {
                let input = self.read_message_from_stdin()?;

                input
                    .lines()
                    .map(|m| Message::new(None, String::from(m).into(), self.get_headers()))
                    .collect()
            }
        };

        client
            .send_messages(
                &self.stream_id,
                &self.topic_id,
                &self.partitioning,
                &mut messages,
            )
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
