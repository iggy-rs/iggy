use crate::bytes_serializable::BytesSerializable;
use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use tracing::{event, Level};

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct DeleteTopic {
    #[serde(skip)]
    pub stream_id: Identifier,
    #[serde(skip)]
    pub topic_id: Identifier,
}

impl CommandPayload for DeleteTopic {}

impl Validatable<Error> for DeleteTopic {
    fn validate(&self) -> std::result::Result<(), Error> {
        Ok(())
    }
}

impl FromStr for DeleteTopic {
    type Err = Error;
    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let topic_id = parts[1].parse::<Identifier>()?;
        let command = DeleteTopic {
            stream_id,
            topic_id,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for DeleteTopic {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(stream_id_bytes.len() + topic_id_bytes.len());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<DeleteTopic, Error> {
        if bytes.len() < 10 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes)?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        let command = DeleteTopic {
            stream_id,
            topic_id,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for DeleteTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.stream_id, self.topic_id)
    }
}

pub struct DeleteTopicCmd {
    delete_topic: DeleteTopic,
}

impl DeleteTopicCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier) -> Self {
        Self {
            delete_topic: DeleteTopic {
                stream_id,
                topic_id,
            },
        }
    }
}

#[async_trait]
impl CliCommand for DeleteTopicCmd {
    fn explain(&self) -> String {
        format!(
            "delete topic with ID: {} in stream with ID: {}",
            self.delete_topic.topic_id, self.delete_topic.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .delete_topic(&self.delete_topic)
            .await
            .with_context(|| {
                format!(
                    "Problem deleting topic with ID: {} in stream {}",
                    self.delete_topic.topic_id, self.delete_topic.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with ID: {} in stream with ID: {} deleted",
            self.delete_topic.topic_id, self.delete_topic.stream_id
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = DeleteTopic {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(&bytes).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let bytes = [stream_id.as_bytes(), topic_id.as_bytes()].concat();
        let command = DeleteTopic::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let input = format!("{stream_id}|{topic_id}");
        let command = DeleteTopic::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
    }
}
