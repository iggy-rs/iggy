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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let is_empty = false;
        let command = PollMessages {
            consumer_id: 1,
            stream_id: 2,
            topic_id: 3,
            partition_id: 4,
            kind: 1,
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
        let kind = bytes[16];
        let value = u64::from_le_bytes(bytes[17..25].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[25..29].try_into().unwrap());
        let auto_commit = bytes[29];
        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };

        assert_eq!(bytes.is_empty(), is_empty);
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
        let is_ok = true;
        let consumer_id = 1u32;
        let stream_id = 2u32;
        let topic_id = 3u32;
        let partition_id = 4u32;
        let kind = 1u8;
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

        bytes.extend(kind.to_le_bytes());
        bytes.extend(value.to_le_bytes());
        bytes.extend(count.to_le_bytes());
        bytes.extend(auto_commit.to_le_bytes());

        let command = PollMessages::from_bytes(&bytes);
        assert_eq!(command.is_ok(), is_ok);

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
        let is_ok = true;
        let consumer_id = 1u32;
        let stream_id = 2u32;
        let topic_id = 3u32;
        let partition_id = 4u32;
        let kind = 1u8;
        let kind_str = "timestamp";
        let value = 2u64;
        let count = 3u32;
        let auto_commit = 1u8;
        let auto_commit_str = "auto_commit";

        let input = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}",
            consumer_id, stream_id, topic_id, partition_id, kind_str, value, count, auto_commit_str
        );
        let command = PollMessages::from_str(&input);
        assert_eq!(command.is_ok(), is_ok);

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
