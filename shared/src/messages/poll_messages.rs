use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;

#[derive(Debug)]
pub struct PollMessages {
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

impl TryFrom<&[&str]> for PollMessages {
    type Error = Error;
    fn try_from(input: &[&str]) -> Result<Self, Self::Error> {
        if input.len() < 6 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = input[0].parse::<u32>()?;
        let topic_id = input[1].parse::<u32>()?;
        let partition_id = input[2].parse::<u32>()?;
        let kind = input[3].parse::<u8>()?;
        let value = input[4].parse::<u64>()?;
        let count = input[5].parse::<u32>()?;
        let format = match input.get(6) {
            Some(format) => match *format {
                "b" => Format::Binary,
                "s" => Format::String,
                _ => return Err(Error::InvalidFormat),
            },
            None => Format::Binary,
        };

        Ok(PollMessages {
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
        let stream_id = &self.stream_id.to_le_bytes();
        let topic_id = &self.topic_id.to_le_bytes();
        let partition_id = &self.partition_id.to_le_bytes();
        let kind = &self.kind.to_le_bytes();
        let value = &self.value.to_le_bytes();
        let count = &self.count.to_le_bytes();

        let bytes: Vec<&[u8]> = vec![stream_id, topic_id, partition_id, kind, value, count];
        bytes.concat()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != 25 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes[..4].try_into()?);
        let topic_id = u32::from_le_bytes(bytes[4..8].try_into()?);
        let partition_id = u32::from_le_bytes(bytes[8..12].try_into()?);
        let kind = bytes[12];
        let value = u64::from_le_bytes(bytes[13..21].try_into()?);
        let count = u32::from_le_bytes(bytes[21..25].try_into()?);

        Ok(PollMessages {
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
