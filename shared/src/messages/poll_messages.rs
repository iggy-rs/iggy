use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use std::str::FromStr;

#[derive(Debug)]
pub struct PollMessages {
    pub consumer_id: u32,
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub kind: u8,
    pub value: u64,
    pub count: u32,
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
        if parts.len() < 6 {
            return Err(Error::InvalidCommand);
        }

        let consumer_id = parts[0].parse::<u32>()?;
        let stream_id = parts[1].parse::<u32>()?;
        let topic_id = parts[2].parse::<u32>()?;
        let partition_id = parts[3].parse::<u32>()?;
        let kind = parts[4].parse::<u8>()?;
        let value = parts[5].parse::<u64>()?;
        let count = parts[6].parse::<u32>()?;
        let format = match parts.get(7) {
            Some(format) => match *format {
                "b" => Format::Binary,
                "s" => Format::String,
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
            format,
        })
    }
}

impl BytesSerializable for PollMessages {
    type Type = PollMessages;

    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(29);
        bytes.extend(self.consumer_id.to_le_bytes());
        bytes.extend(self.stream_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        bytes.extend(self.partition_id.to_le_bytes());
        bytes.extend(self.kind.to_le_bytes());
        bytes.extend(self.value.to_le_bytes());
        bytes.extend(self.count.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != 29 {
            return Err(Error::InvalidCommand);
        }

        let consumer_id = u32::from_le_bytes(bytes[..4].try_into()?);
        let stream_id = u32::from_le_bytes(bytes[4..8].try_into()?);
        let topic_id = u32::from_le_bytes(bytes[8..12].try_into()?);
        let partition_id = u32::from_le_bytes(bytes[12..16].try_into()?);
        let kind = bytes[16];
        let value = u64::from_le_bytes(bytes[17..25].try_into()?);
        let count = u32::from_le_bytes(bytes[25..29].try_into()?);

        Ok(PollMessages {
            consumer_id,
            stream_id,
            topic_id,
            partition_id,
            kind,
            value,
            count,
            format: Format::None,
        })
    }
}
