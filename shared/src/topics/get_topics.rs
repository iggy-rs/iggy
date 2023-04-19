use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;

#[derive(Debug)]
pub struct GetTopics {
    pub stream_id: u32,
}

impl TryFrom<&[&str]> for GetTopics {
    type Error = Error;
    fn try_from(input: &[&str]) -> Result<Self, Self::Error> {
        if input.len() != 1 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = input[0].parse::<u32>()?;

        Ok(GetTopics { stream_id })
    }
}

impl BytesSerializable for GetTopics {
    type Type = GetTopics;

    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4);
        bytes.extend_from_slice(&self.stream_id.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, Error> {
        if bytes.len() != 4 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes.try_into()?);

        Ok(GetTopics { stream_id })
    }
}
