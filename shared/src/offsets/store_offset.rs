use crate::bytes_serializable::BytesSerializable;
use crate::command::STORE_OFFSET;
use crate::error::Error;
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug)]
pub struct StoreOffset {
    pub consumer_id: u32,
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub offset: u64,
}

impl FromStr for StoreOffset {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 5 {
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
        let offset = parts[4].parse::<u64>()?;

        Ok(StoreOffset {
            consumer_id,
            stream_id,
            topic_id,
            partition_id,
            offset,
        })
    }
}

impl BytesSerializable for StoreOffset {
    type Type = StoreOffset;

    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(24);
        bytes.extend(self.consumer_id.to_le_bytes());
        bytes.extend(self.stream_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        bytes.extend(self.partition_id.to_le_bytes());
        bytes.extend(self.offset.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, Error> {
        if bytes.len() != 24 {
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
        let offset = u64::from_le_bytes(bytes[16..24].try_into()?);

        Ok(StoreOffset {
            consumer_id,
            stream_id,
            topic_id,
            partition_id,
            offset,
        })
    }
}

impl Display for StoreOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} → consumer ID: {}, stream ID: {}, topic ID: {}, partition ID: {}, offset: {}",
            STORE_OFFSET,
            self.consumer_id,
            self.stream_id,
            self.topic_id,
            self.partition_id,
            self.offset
        )
    }
}
