use crate::bytes_serializable::BytesSerializable;
use crate::command::GET_TOPICS;
use crate::error::Error;
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug)]
pub struct GetTopics {
    pub stream_id: u32,
}

impl FromStr for GetTopics {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 1 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<u32>()?;
        if stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        Ok(GetTopics { stream_id })
    }
}

impl BytesSerializable for GetTopics {
    type Type = GetTopics;

    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4);
        bytes.extend(self.stream_id.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, Error> {
        if bytes.len() != 4 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes.try_into()?);
        if stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        Ok(GetTopics { stream_id })
    }
}

impl Display for GetTopics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} → stream ID: {}", GET_TOPICS, self.stream_id)
    }
}
