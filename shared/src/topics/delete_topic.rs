use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;

#[derive(Debug)]
pub struct DeleteTopic {
    pub stream_id: u32,
    pub topic_id: u32,
}

impl TryFrom<&[&str]> for DeleteTopic {
    type Error = Error;
    fn try_from(input: &[&str]) -> Result<Self, Self::Error> {
        if input.len() != 2 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = input[0].parse::<u32>()?;
        let topic_id = input[1].parse::<u32>()?;

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
