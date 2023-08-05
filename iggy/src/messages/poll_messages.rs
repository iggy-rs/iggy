use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::consumer::{Consumer, ConsumerKind};
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct PollMessages {
    #[serde(flatten)]
    pub consumer: Consumer,
    #[serde(skip)]
    pub stream_id: Identifier,
    #[serde(skip)]
    pub topic_id: Identifier,
    #[serde(default = "default_partition_id")]
    pub partition_id: u32,
    #[serde(default = "default_kind")]
    pub kind: PollingKind,
    #[serde(default = "default_value")]
    pub value: u64,
    #[serde(default = "default_count")]
    pub count: u32,
    #[serde(default)]
    pub auto_commit: bool,
    #[serde(skip)]
    pub format: Format,
}

impl Default for PollMessages {
    fn default() -> Self {
        Self {
            consumer: Consumer::default(),
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(1).unwrap(),
            partition_id: default_partition_id(),
            kind: default_kind(),
            value: default_value(),
            count: default_count(),
            auto_commit: false,
            format: Format::None,
        }
    }
}

impl CommandPayload for PollMessages {}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PollingKind {
    #[default]
    Offset,
    Timestamp,
    First,
    Last,
    Next,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone)]
pub enum Format {
    #[default]
    None,
    Binary,
    String,
}

fn default_partition_id() -> u32 {
    1
}

fn default_kind() -> PollingKind {
    PollingKind::Offset
}

fn default_value() -> u64 {
    0
}

fn default_count() -> u32 {
    10
}

impl Validatable for PollMessages {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl PollingKind {
    pub fn as_code(&self) -> u8 {
        match self {
            PollingKind::Offset => 1,
            PollingKind::Timestamp => 2,
            PollingKind::First => 3,
            PollingKind::Last => 4,
            PollingKind::Next => 5,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            1 => Ok(PollingKind::Offset),
            2 => Ok(PollingKind::Timestamp),
            3 => Ok(PollingKind::First),
            4 => Ok(PollingKind::Last),
            5 => Ok(PollingKind::Next),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl FromStr for PollingKind {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "o" | "offset" => Ok(PollingKind::Offset),
            "t" | "timestamp" => Ok(PollingKind::Timestamp),
            "f" | "first" => Ok(PollingKind::First),
            "l" | "last" => Ok(PollingKind::Last),
            "n" | "next" => Ok(PollingKind::Next),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Display for PollingKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollingKind::Offset => write!(f, "offset"),
            PollingKind::Timestamp => write!(f, "timestamp"),
            PollingKind::First => write!(f, "first"),
            PollingKind::Last => write!(f, "last"),
            PollingKind::Next => write!(f, "next"),
        }
    }
}

impl FromStr for PollMessages {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() < 8 {
            return Err(Error::InvalidCommand);
        }

        let consumer_kind = ConsumerKind::from_str(parts[0])?;
        let consumer_id = parts[1].parse::<u32>()?;
        let consumer = Consumer {
            kind: consumer_kind,
            id: consumer_id,
        };
        let stream_id = parts[2].parse::<Identifier>()?;
        let topic_id = parts[3].parse::<Identifier>()?;
        let partition_id = parts[4].parse::<u32>()?;
        let kind = PollingKind::from_str(parts[5])?;
        let value = parts[6].parse::<u64>()?;
        let count = parts[7].parse::<u32>()?;
        let auto_commit = match parts.get(8) {
            Some(auto_commit) => match *auto_commit {
                "a" | "auto_commit" => true,
                "n" | "no_commit" => false,
                _ => return Err(Error::InvalidCommand),
            },
            None => false,
        };
        let format = match parts.get(9) {
            Some(format) => match *format {
                "b" | "binary" => Format::Binary,
                "s" | "string" => Format::String,
                _ => return Err(Error::InvalidFormat),
            },
            None => Format::None,
        };

        let command = PollMessages {
            consumer,
            stream_id,
            topic_id,
            partition_id,
            kind,
            value,
            count,
            auto_commit,
            format,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for PollMessages {
    fn as_bytes(&self) -> Vec<u8> {
        let consumer_bytes = self.consumer.as_bytes();
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(
            18 + consumer_bytes.len() + stream_id_bytes.len() + topic_id_bytes.len(),
        );
        bytes.extend(consumer_bytes);
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(self.partition_id.to_le_bytes());
        bytes.extend(self.kind.as_code().to_le_bytes());
        bytes.extend(self.value.to_le_bytes());
        bytes.extend(self.count.to_le_bytes());
        if self.auto_commit {
            bytes.extend(1u8.to_le_bytes());
        } else {
            bytes.extend(0u8.to_le_bytes());
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 29 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let consumer_kind = ConsumerKind::from_code(bytes[0])?;
        let consumer_id = u32::from_le_bytes(bytes[1..5].try_into()?);
        let consumer = Consumer {
            kind: consumer_kind,
            id: consumer_id,
        };
        position += 5;
        let stream_id = Identifier::from_bytes(&bytes[position..])?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        position += topic_id.get_size_bytes() as usize;
        let partition_id = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let kind = PollingKind::from_code(bytes[position + 4])?;
        position += 5;
        let value = u64::from_le_bytes(bytes[position..position + 8].try_into()?);
        let count = u32::from_le_bytes(bytes[position + 8..position + 12].try_into()?);
        let auto_commit = bytes[position + 12];
        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };
        let format = Format::None;
        let command = PollMessages {
            consumer,
            stream_id,
            topic_id,
            partition_id,
            kind,
            value,
            count,
            auto_commit,
            format,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for PollMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{}|{}",
            self.consumer,
            self.stream_id,
            self.topic_id,
            self.partition_id,
            self.kind,
            self.value,
            self.count,
            auto_commit_to_string(self.auto_commit)
        )
    }
}

fn auto_commit_to_string(auto_commit: bool) -> &'static str {
    if auto_commit {
        "a"
    } else {
        "n"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = PollMessages {
            consumer: Consumer::new(1),
            stream_id: Identifier::numeric(2).unwrap(),
            topic_id: Identifier::numeric(3).unwrap(),
            partition_id: 4,
            kind: PollingKind::Offset,
            value: 2,
            count: 3,
            auto_commit: true,
            format: Format::Binary,
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let consumer_kind = ConsumerKind::from_code(bytes[0]).unwrap();
        let consumer_id = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
        let consumer = Consumer {
            kind: consumer_kind,
            id: consumer_id,
        };
        position += 5;
        let stream_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let partition_id = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let kind = PollingKind::from_code(bytes[position + 4]).unwrap();
        position += 5;
        let value = u64::from_le_bytes(bytes[position..position + 8].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[position + 8..position + 12].try_into().unwrap());
        let auto_commit = bytes[position + 12];
        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };

        assert!(!bytes.is_empty());
        assert_eq!(consumer, command.consumer);
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partition_id, command.partition_id);
        assert_eq!(kind, command.kind);
        assert_eq!(value, command.value);
        assert_eq!(count, command.count);
        assert_eq!(auto_commit, command.auto_commit);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let consumer = Consumer::new(1);
        let stream_id = Identifier::numeric(2).unwrap();
        let topic_id = Identifier::numeric(3).unwrap();
        let partition_id = 4u32;
        let kind = PollingKind::Offset;
        let value = 2u64;
        let count = 3u32;
        let auto_commit = 1u8;

        let consumer_bytes = consumer.as_bytes();
        let stream_id_bytes = stream_id.as_bytes();
        let topic_id_bytes = topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(
            18 + consumer_bytes.len() + stream_id_bytes.len() + topic_id_bytes.len(),
        );
        bytes.extend(consumer_bytes);
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(partition_id.to_le_bytes());
        bytes.extend(kind.as_code().to_le_bytes());
        bytes.extend(value.to_le_bytes());
        bytes.extend(count.to_le_bytes());
        bytes.extend(auto_commit.to_le_bytes());

        let command = PollMessages::from_bytes(&bytes);
        assert!(command.is_ok());

        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };

        let command = command.unwrap();
        assert_eq!(command.consumer, consumer);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, partition_id);
        assert_eq!(command.kind, kind);
        assert_eq!(command.value, value);
        assert_eq!(command.count, count);
        assert_eq!(command.auto_commit, auto_commit);
    }

    #[test]
    fn should_be_read_from_string() {
        let consumer = Consumer::new(1);
        let stream_id = Identifier::numeric(2).unwrap();
        let topic_id = Identifier::numeric(3).unwrap();
        let partition_id = 4u32;
        let kind = PollingKind::Timestamp;
        let value = 2u64;
        let count = 3u32;
        let auto_commit = 1u8;
        let auto_commit_str = "auto_commit";

        let input = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}",
            consumer, stream_id, topic_id, partition_id, kind, value, count, auto_commit_str
        );
        let command = PollMessages::from_str(&input);
        assert!(command.is_ok());

        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };

        let command = command.unwrap();
        assert_eq!(command.consumer, consumer);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, partition_id);
        assert_eq!(command.kind, kind);
        assert_eq!(command.value, value);
        assert_eq!(command.count, count);
        assert_eq!(command.auto_commit, auto_commit);
    }
}
