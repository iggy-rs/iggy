use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;

pub const MAX_PAYLOAD_SIZE: usize = 1000;

#[derive(Debug)]
pub struct SendMessage {
    pub stream_id: u32,
    pub topic_id: u32,
    pub key_kind: u8,
    pub key_value: u32,
    pub payload: Vec<u8>,
}

impl TryFrom<&[&str]> for SendMessage {
    type Error = Error;
    fn try_from(input: &[&str]) -> Result<Self, Self::Error> {
        if input.len() != 5 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = input[0].parse::<u32>()?;
        let topic_id = input[1].parse::<u32>()?;
        let key_kind = input[2].parse::<u8>()?;
        let key_value = input[3].parse::<u32>()?;
        let payload = input[4].as_bytes().to_vec();

        if payload.len() > MAX_PAYLOAD_SIZE {
            return Err(Error::TooBigPayload);
        }

        Ok(SendMessage {
            stream_id,
            topic_id,
            key_kind,
            key_value,
            payload,
        })
    }
}

impl BytesSerializable for SendMessage {
    type Type = SendMessage;

    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(13 + self.payload.len());
        bytes.extend_from_slice(&self.stream_id.to_le_bytes());
        bytes.extend_from_slice(&self.topic_id.to_le_bytes());
        bytes.extend_from_slice(&self.key_kind.to_le_bytes());
        bytes.extend_from_slice(&self.key_value.to_le_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, Error> {
        if bytes.len() < 14 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes[..4].try_into()?);
        let topic_id = u32::from_le_bytes(bytes[4..8].try_into()?);
        let key_kind = bytes[8];
        let key_value = u32::from_le_bytes(bytes[9..13].try_into()?);
        let payload = bytes[13..].to_vec();

        if payload.len() > MAX_PAYLOAD_SIZE {
            return Err(Error::TooBigPayload);
        }

        Ok(SendMessage {
            stream_id,
            topic_id,
            key_kind,
            key_value,
            payload,
        })
    }
}
