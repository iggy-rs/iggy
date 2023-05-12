use crate::bytes_serializable::BytesSerializable;
use crate::command::POLL_MESSAGES;
use crate::error::Error;
use std::fmt::Display;
use std::str::FromStr;

// TODO: Extend with consumer group.
#[derive(Debug)]
pub struct PollMessages {
    pub consumer_id: u32,
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub kind: u8,
    pub value: u64,
    pub count: u32,
    pub auto_commit: bool,
    pub format: Format,
}

#[derive(Debug, PartialEq)]
pub enum Format {
    None,
    Binary,
    String,
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
        if stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        let topic_id = parts[2].parse::<u32>()?;
        if topic_id == 0 {
            return Err(Error::InvalidTopicId);
        }

        let partition_id = parts[3].parse::<u32>()?;
        let kind = match parts[4] {
            "o" | "offset" => 0,
            "t" | "timestamp" => 1,
            "f" | "first" => 2,
            "l" | "last" => 3,
            "n" | "next" => 4,
            _ => return Err(Error::InvalidCommand),
        };

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
            None => Format::Binary,
        };

        Ok(PollMessages {
            consumer_id,
            stream_id,
            topic_id,
            partition_id,
            kind,
            value,
            count,
            auto_commit,
            format,
        })
    }
}

impl BytesSerializable for PollMessages {
    type Type = PollMessages;

    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(30);
        bytes.extend(self.consumer_id.to_le_bytes());
        bytes.extend(self.stream_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        bytes.extend(self.partition_id.to_le_bytes());
        bytes.extend(self.kind.to_le_bytes());
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
        if stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        let topic_id = u32::from_le_bytes(bytes[8..12].try_into()?);
        if topic_id == 0 {
            return Err(Error::InvalidTopicId);
        }

        let partition_id = u32::from_le_bytes(bytes[12..16].try_into()?);
        let kind = bytes[16];
        let value = u64::from_le_bytes(bytes[17..25].try_into()?);
        let count = u32::from_le_bytes(bytes[25..29].try_into()?);
        let auto_commit = bytes[29];
        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };

        Ok(PollMessages {
            consumer_id,
            stream_id,
            topic_id,
            partition_id,
            kind,
            value,
            count,
            auto_commit,
            format: Format::None,
        })
    }
}

impl Display for PollMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} → consumer ID: {}, stream ID: {}, topic ID: {}, partition ID: {}, kind: {}, value: {}, count: {}, auto commit: {}",
            POLL_MESSAGES, self.consumer_id, self.stream_id, self.topic_id, self.partition_id, self.kind, self.value, self.count, self.auto_commit
        )
    }
}
