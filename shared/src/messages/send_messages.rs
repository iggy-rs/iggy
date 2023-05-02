use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use std::str::FromStr;

pub const MAX_PAYLOAD_SIZE: usize = 1024 * 1024 * 1024;

#[derive(Debug)]
pub struct SendMessages {
    pub stream_id: u32,
    pub topic_id: u32,
    pub key_kind: u8,
    pub key_value: u32,
    pub messages_count: u32,
    pub messages: Vec<Message>,
}

#[derive(Debug)]
pub struct Message {
    pub length: u32,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn get_size_bytes(&self) -> u32 {
        // Length + Payload
        4 + self.payload.len() as u32
    }
}

impl FromStr for SendMessages {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 5 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<u32>()?;
        let topic_id = parts[1].parse::<u32>()?;
        let key_kind = parts[2].parse::<u8>()?;
        let key_value = parts[3].parse::<u32>()?;
        let payload = parts[4].as_bytes().to_vec();

        if payload.len() > MAX_PAYLOAD_SIZE {
            return Err(Error::TooBigPayload);
        }

        // For now, we only support a single payload.
        Ok(SendMessages {
            stream_id,
            topic_id,
            key_kind,
            key_value,
            messages_count: 1,
            messages: vec![Message {
                length: payload.len() as u32,
                payload,
            }],
        })
    }
}

impl BytesSerializable for SendMessages {
    type Type = SendMessages;

    fn as_bytes(&self) -> Vec<u8> {
        let payloads_size = self
            .messages
            .iter()
            .map(|payload| payload.get_size_bytes())
            .sum::<u32>();

        let mut bytes = Vec::with_capacity(17 + payloads_size as usize);
        bytes.extend(self.stream_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        bytes.extend(self.key_kind.to_le_bytes());
        bytes.extend(self.key_value.to_le_bytes());
        bytes.extend(self.messages_count.to_le_bytes());
        for message in &self.messages {
            bytes.extend(message.length.to_le_bytes());
            bytes.extend(&message.payload);
        }

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
        let messages_count = u32::from_le_bytes(bytes[13..17].try_into()?);
        let messages_payloads = &bytes[17..];

        if messages_payloads.len() > MAX_PAYLOAD_SIZE {
            return Err(Error::TooBigPayload);
        }

        let mut position = 0;
        let mut messages = Vec::with_capacity(messages_count as usize);
        while position < messages_payloads.len() - 4 {
            let length = u32::from_le_bytes(messages_payloads[position..position + 4].try_into()?);
            position += 4;
            let payload = messages_payloads[position..position + length as usize].to_vec();
            position += length as usize;
            messages.push(Message { length, payload });
        }

        Ok(SendMessages {
            stream_id,
            topic_id,
            key_kind,
            key_value,
            messages_count,
            messages,
        })
    }
}
