use crate::bytes_serializable::BytesSerializable;
use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use anyhow::Context;
use async_trait::async_trait;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use tracing::{event, Level};

const MAX_PARTITIONS_COUNT: u32 = 100_000;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CreatePartitions {
    #[serde(skip)]
    pub stream_id: Identifier,
    #[serde(skip)]
    pub topic_id: Identifier,
    pub partitions_count: u32,
}

impl CommandPayload for CreatePartitions {}

impl Default for CreatePartitions {
    fn default() -> Self {
        CreatePartitions {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partitions_count: 1,
        }
    }
}

impl Validatable<Error> for CreatePartitions {
    fn validate(&self) -> std::result::Result<(), Error> {
        if !(1..=MAX_PARTITIONS_COUNT).contains(&self.partitions_count) {
            return Err(Error::TooManyPartitions);
        }

        Ok(())
    }
}

impl FromStr for CreatePartitions {
    type Err = Error;
    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 3 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let topic_id = parts[1].parse::<Identifier>()?;
        let partitions_count = parts[2].parse::<u32>()?;
        let command = CreatePartitions {
            stream_id,
            topic_id,
            partitions_count,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for CreatePartitions {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(4 + stream_id_bytes.len() + topic_id_bytes.len());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.put_u32_le(self.partitions_count);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<CreatePartitions, Error> {
        if bytes.len() < 10 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes)?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        position += topic_id.get_size_bytes() as usize;
        let partitions_count = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let command = CreatePartitions {
            stream_id,
            topic_id,
            partitions_count,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for CreatePartitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}",
            self.stream_id, self.topic_id, self.partitions_count
        )
    }
}

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
        format!(
            "create {} partitions for topic with ID: {} and stream with ID: {}",
            self.create_partition.partitions_count,
            self.create_partition.topic_id,
            self.create_partition.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_partitions(&self.create_partition)
            .await
            .with_context(|| {
                format!(
                    "Problem creating {} partitions for topic with ID: {} and stream with ID: {}",
                    self.create_partition.partitions_count,
                    self.create_partition.topic_id,
                    self.create_partition.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Created {} partitions for topic with ID: {} and stream with ID: {}",
            self.create_partition.partitions_count,
            self.create_partition.topic_id,
            self.create_partition.stream_id,
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreatePartitions {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            partitions_count: 3,
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(&bytes).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let partitions_count =
            u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partitions_count, command.partitions_count);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let partitions_count = 3u32;
        let stream_id_bytes = stream_id.as_bytes();
        let topic_id_bytes = topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(4 + stream_id_bytes.len() + topic_id_bytes.len());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.put_u32_le(partitions_count);
        let command = CreatePartitions::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partitions_count, partitions_count);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let partitions_count = 3u32;
        let input = format!("{stream_id}|{topic_id}|{partitions_count}");
        let command = CreatePartitions::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partitions_count, partitions_count);
    }
}
