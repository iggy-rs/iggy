use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use std::str::FromStr;

#[derive(Debug)]
pub struct DeleteTopic {
    pub stream_id: u32,
    pub topic_id: u32,
}

impl FromStr for DeleteTopic {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<u32>()?;
        let topic_id = parts[1].parse::<u32>()?;

        Ok(DeleteTopic {
            stream_id,
            topic_id,
        })
    }
}

impl BytesSerializable for DeleteTopic {
    type Type = DeleteTopic;

    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8);
        bytes.extend_from_slice(&self.stream_id.to_le_bytes());
        bytes.extend_from_slice(&self.topic_id.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, Error> {
        if bytes.len() != 8 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes[..4].try_into()?);
        let topic_id = u32::from_le_bytes(bytes[4..8].try_into()?);

        Ok(DeleteTopic {
            stream_id,
            topic_id,
        })
    }
}
