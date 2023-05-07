use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use std::str::FromStr;

#[derive(Debug)]
pub struct StoreOffset {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub offset: u64,
}

impl FromStr for StoreOffset {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 4 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<u32>()?;
        let topic_id = parts[1].parse::<u32>()?;
        let partition_id = parts[2].parse::<u32>()?;
        let offset = parts[3].parse::<u64>()?;

        Ok(StoreOffset {
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
        let mut bytes = Vec::with_capacity(20);
        bytes.extend(self.stream_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        bytes.extend(self.partition_id.to_le_bytes());
        bytes.extend(self.offset.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, Error> {
        if bytes.len() != 20 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes[..4].try_into()?);
        let topic_id = u32::from_le_bytes(bytes[4..8].try_into()?);
        let partition_id = u32::from_le_bytes(bytes[8..12].try_into()?);
        let offset = u64::from_le_bytes(bytes[12..20].try_into()?);

        Ok(StoreOffset {
            stream_id,
            topic_id,
            partition_id,
            offset,
        })
    }
}
