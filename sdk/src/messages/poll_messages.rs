use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

// TODO: Extend with consumer group.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct PollMessages {
    #[serde(default = "default_consumer_id")]
    pub consumer_id: u32,
    #[serde(skip)]
    pub stream_id: u32,
    #[serde(skip)]
    pub topic_id: u32,
    #[serde(default = "default_partition_id")]
    pub partition_id: u32,
    #[serde(default = "default_kind")]
    pub kind: Kind,
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
            consumer_id: default_consumer_id(),
            stream_id: 1,
            topic_id: 1,
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
pub enum Kind {
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

fn default_consumer_id() -> u32 {
    0
}

fn default_partition_id() -> u32 {
    1
}

fn default_kind() -> Kind {
    Kind::Offset
}

fn default_value() -> u64 {
    0
}

fn default_count() -> u32 {
    10
}

impl Validatable for PollMessages {
    fn validate(&self) -> Result<(), Error> {
        if self.stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        if self.topic_id == 0 {
            return Err(Error::InvalidTopicId);
        }

        Ok(())
    }
}

impl Kind {
    pub fn as_code(&self) -> u8 {
        match self {
            Kind::Offset => 0,
            Kind::Timestamp => 1,
            Kind::First => 2,
            Kind::Last => 3,
            Kind::Next => 4,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            0 => Ok(Kind::Offset),
            1 => Ok(Kind::Timestamp),
            2 => Ok(Kind::First),
            3 => Ok(Kind::Last),
            4 => Ok(Kind::Next),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl FromStr for Kind {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "o" | "offset" => Ok(Kind::Offset),
            "t" | "timestamp" => Ok(Kind::Timestamp),
            "f" | "first" => Ok(Kind::First),
            "l" | "last" => Ok(Kind::Last),
            "n" | "next" => Ok(Kind::Next),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl FromStr for PollMessages {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() < 7 {
            return Err(Error::InvalidCommand);
        }

        let consumer_id = parts[0].parse::<u32>()?;
        let stream_id = parts[1].parse::<u32>()?;
        let topic_id = parts[2].parse::<u32>()?;
        let partition_id = parts[3].parse::<u32>()?;
        let kind = parts[4];
        let kind = Kind::from_str(kind)?;
        let value = parts[5].parse::<u64>()?;
        let count = parts[6].parse::<u32>()?;
        let auto_commit = match parts.get(7) {
            Some(auto_commit) => match *auto_commit {
                "a" | "auto_commit" => true,
                "n" | "no_commit" => false,
                _ => return Err(Error::InvalidCommand),
            },
            None => false,
        };
        let format = match parts.get(8) {
            Some(format) => match *format {
                "b" | "binary" => Format::Binary,
                "s" | "string" => Format::String,
                _ => return Err(Error::InvalidFormat),
            },
            None => Format::None,
        };

        let command = PollMessages {
            consumer_id,
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
        let mut bytes = Vec::with_capacity(30);
        bytes.extend(self.consumer_id.to_le_bytes());
        bytes.extend(self.stream_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
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
        if bytes.len() != 30 {
            return Err(Error::InvalidCommand);
        }

        let consumer_id = u32::from_le_bytes(bytes[..4].try_into()?);
        let stream_id = u32::from_le_bytes(bytes[4..8].try_into()?);
        let topic_id = u32::from_le_bytes(bytes[8..12].try_into()?);
        let partition_id = u32::from_le_bytes(bytes[12..16].try_into()?);
        let kind = bytes[16];
        let kind = Kind::from_code(kind)?;
        let value = u64::from_le_bytes(bytes[17..25].try_into()?);
        let count = u32::from_le_bytes(bytes[25..29].try_into()?);
        let auto_commit = bytes[29];
        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };
        let format = Format::None;

        let command = PollMessages {
            consumer_id,
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
            self.consumer_id,
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

impl Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Kind::Offset => write!(f, "offset"),
            Kind::Timestamp => write!(f, "timestamp"),
            Kind::First => write!(f, "first"),
            Kind::Last => write!(f, "last"),
            Kind::Next => write!(f, "next"),
        }
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
            consumer_id: 1,
            stream_id: 2,
            topic_id: 3,
            partition_id: 4,
            kind: Kind::Offset,
            value: 2,
            count: 3,
            auto_commit: true,
            format: Format::Binary,
        };

        let bytes = command.as_bytes();
        let consumer_id = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        let stream_id = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let topic_id = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let partition_id = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let kind = Kind::from_code(bytes[16]).unwrap();
        let value = u64::from_le_bytes(bytes[17..25].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[25..29].try_into().unwrap());
        let auto_commit = bytes[29];
        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };

        assert!(!bytes.is_empty());
        assert_eq!(consumer_id, command.consumer_id);
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
        let consumer_id = 1u32;
        let stream_id = 2u32;
        let topic_id = 3u32;
        let partition_id = 4u32;
        let kind = Kind::Offset;
        let value = 2u64;
        let count = 3u32;
        let auto_commit = 1u8;
        let mut bytes: Vec<u8> = [
            consumer_id.to_le_bytes(),
            stream_id.to_le_bytes(),
            topic_id.to_le_bytes(),
            partition_id.to_le_bytes(),
        ]
        .concat();

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
        assert_eq!(command.consumer_id, consumer_id);
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
        let consumer_id = 1u32;
        let stream_id = 2u32;
        let topic_id = 3u32;
        let partition_id = 4u32;
        let kind = Kind::Timestamp;
        let value = 2u64;
        let count = 3u32;
        let auto_commit = 1u8;
        let auto_commit_str = "auto_commit";

        let input = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}",
            consumer_id, stream_id, topic_id, partition_id, kind, value, count, auto_commit_str
        );
        let command = PollMessages::from_str(&input);
        assert!(command.is_ok());

        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };

        let command = command.unwrap();
        assert_eq!(command.consumer_id, consumer_id);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, partition_id);
        assert_eq!(command.kind, kind);
        assert_eq!(command.value, value);
        assert_eq!(command.count, count);
        assert_eq!(command.auto_commit, auto_commit);
    }
}
