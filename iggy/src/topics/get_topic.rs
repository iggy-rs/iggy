use crate::bytes_serializable::BytesSerializable;
use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::utils::timestamp::TimeStamp;
use crate::validatable::Validatable;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use tracing::{event, Level};

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct GetTopic {
    #[serde(skip)]
    pub stream_id: Identifier,
    #[serde(skip)]
    pub topic_id: Identifier,
}

impl CommandPayload for GetTopic {}

impl Validatable<Error> for GetTopic {
    fn validate(&self) -> std::result::Result<(), Error> {
        Ok(())
    }
}

impl FromStr for GetTopic {
    type Err = Error;
    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let topic_id = parts[1].parse::<Identifier>()?;
        let command = GetTopic {
            stream_id,
            topic_id,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for GetTopic {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(stream_id_bytes.len() + topic_id_bytes.len());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<GetTopic, Error> {
        if bytes.len() < 6 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes)?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        let command = GetTopic {
            stream_id,
            topic_id,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for GetTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.stream_id, self.topic_id)
    }
}

pub struct GetTopicCmd {
    get_topic: GetTopic,
}

impl GetTopicCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier) -> Self {
        Self {
            get_topic: GetTopic {
                stream_id,
                topic_id,
            },
        }
    }
}

#[async_trait]
impl CliCommand for GetTopicCmd {
    fn explain(&self) -> String {
        format!(
            "get topic with ID: {} from stream with ID: {}",
            self.get_topic.topic_id, self.get_topic.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let topic = client.get_topic(&self.get_topic).await.with_context(|| {
            format!(
                "Problem getting topic with ID: {} in stream {}",
                self.get_topic.topic_id, self.get_topic.stream_id
            )
        })?;

        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec!["Topic id", format!("{}", topic.id).as_str()]);
        table.add_row(vec![
            "Created",
            TimeStamp::from(topic.created_at)
                .to_string("%Y-%m-%d %H:%M:%S")
                .as_str(),
        ]);
        table.add_row(vec!["Topic name", topic.name.as_str()]);
        table.add_row(vec!["Topic size", format!("{}", topic.size_bytes).as_str()]);
        table.add_row(vec![
            "Message expiry",
            match topic.message_expiry {
                Some(value) => format!("{}", value),
                None => String::from("None"),
            }
            .as_str(),
        ]);
        table.add_row(vec![
            "Topic message count",
            format!("{}", topic.messages_count).as_str(),
        ]);
        table.add_row(vec![
            "Partitions count",
            format!("{}", topic.partitions_count).as_str(),
        ]);

        event!(target: PRINT_TARGET, Level::INFO,"{table}");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = GetTopic {
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
        let command = GetTopic::from_bytes(&bytes);
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
        let command = GetTopic::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
    }
}
