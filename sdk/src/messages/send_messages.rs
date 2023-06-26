use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::validatable::Validatable;
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::fmt::Display;
use std::str::FromStr;

const MAX_PAYLOAD_SIZE: u32 = 10 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SendMessages {
    #[serde(skip)]
    pub stream_id: u32,
    #[serde(skip)]
    pub topic_id: u32,
    pub key_kind: KeyKind,
    pub key_value: u32,
    #[serde(skip)]
    pub messages_count: u32,
    pub messages: Vec<Message>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Message {
    #[serde(default = "default_message_id")]
    pub id: u128,
    #[serde(skip)]
    pub length: u32,
    #[serde_as(as = "Base64")]
    pub payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum KeyKind {
    #[default]
    PartitionId,
    EntityId,
}

fn default_message_id() -> u128 {
    0
}

impl Default for SendMessages {
    fn default() -> Self {
        SendMessages {
            stream_id: 1,
            topic_id: 1,
            key_kind: KeyKind::default(),
            key_value: 1,
            messages_count: 1,
            messages: vec![Message::default()],
        }
    }
}

impl CommandPayload for SendMessages {}

impl Validatable for SendMessages {
    fn validate(&self) -> Result<(), Error> {
        if self.stream_id == 0 {
            return Err(Error::InvalidStreamId);
        }

        if self.topic_id == 0 {
            return Err(Error::InvalidTopicId);
        }

        if self.messages_count == 0 {
            return Err(Error::InvalidMessagesCount);
        }

        let mut payload_size = 0;
        for message in &self.messages {
            payload_size += message.payload.len() as u32;
            if payload_size > MAX_PAYLOAD_SIZE {
                return Err(Error::TooBigMessagePayload);
            }
        }

        if payload_size == 0 {
            return Err(Error::EmptyMessagePayload);
        }

        Ok(())
    }
}

impl KeyKind {
    pub fn as_code(&self) -> u8 {
        match self {
            KeyKind::PartitionId => 0,
            KeyKind::EntityId => 1,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            0 => Ok(KeyKind::PartitionId),
            1 => Ok(KeyKind::EntityId),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl FromStr for KeyKind {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "p" | "partition_id" => Ok(KeyKind::PartitionId),
            "c" | "entity_id" => Ok(KeyKind::EntityId),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Message {
    pub fn get_size_bytes(&self) -> u32 {
        // ID + Length + Payload
        16 + 4 + self.payload.len() as u32
    }
}

impl Default for Message {
    fn default() -> Self {
        let payload = "hello world".as_bytes().to_vec();
        Message {
            id: 0,
            length: payload.len() as u32,
            payload,
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.id, String::from_utf8_lossy(&self.payload))
    }
}

impl BytesSerializable for Message {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.get_size_bytes() as usize);
        bytes.extend(self.id.to_le_bytes());
        bytes.extend(self.length.to_le_bytes());
        bytes.extend(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 20 {
            return Err(Error::InvalidCommand);
        }

        let id = u128::from_le_bytes(bytes[..16].try_into()?);
        let length = u32::from_le_bytes(bytes[16..20].try_into()?);
        if length == 0 {
            return Err(Error::EmptyMessagePayload);
        }

        let payload = bytes[20..20 + length as usize].to_vec();
        if payload.len() != length as usize {
            return Err(Error::InvalidMessagePayloadLength);
        }

        Ok(Message {
            id,
            length,
            payload,
        })
    }
}

impl FromStr for Message {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        let (id, payload) = match parts.len() {
            1 => (0, parts[0].as_bytes().to_vec()),
            2 => (parts[0].parse::<u128>()?, parts[1].as_bytes().to_vec()),
            _ => return Err(Error::InvalidCommand),
        };
        let length = payload.len() as u32;
        if length == 0 {
            return Err(Error::EmptyMessagePayload);
        }

        Ok(Message {
            id,
            length,
            payload,
        })
    }
}

impl FromStr for SendMessages {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 6 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<u32>()?;
        let topic_id = parts[1].parse::<u32>()?;
        let key_kind = parts[2];
        let key_kind = KeyKind::from_str(key_kind)?;
        let key_value = parts[3].parse::<u32>()?;
        let message_id = parts[4].parse::<u128>()?;
        let payload = parts[5].as_bytes().to_vec();

        // For now, we only support a single payload.
        let messages_count = 1;
        let message = Message {
            id: message_id,
            length: payload.len() as u32,
            payload,
        };

        let command = SendMessages {
            stream_id,
            topic_id,
            key_kind,
            key_value,
            messages_count,
            messages: vec![message],
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for SendMessages {
    fn as_bytes(&self) -> Vec<u8> {
        let messages_size = self
            .messages
            .iter()
            .map(|message| message.get_size_bytes())
            .sum::<u32>();

        let mut bytes = Vec::with_capacity(17 + messages_size as usize);
        bytes.extend(self.stream_id.to_le_bytes());
        bytes.extend(self.topic_id.to_le_bytes());
        bytes.extend(self.key_kind.as_code().to_le_bytes());
        bytes.extend(self.key_value.to_le_bytes());
        bytes.extend(self.messages_count.to_le_bytes());
        for message in &self.messages {
            bytes.extend(message.id.to_le_bytes());
            bytes.extend(message.length.to_le_bytes());
            bytes.extend(&message.payload);
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<SendMessages, Error> {
        if bytes.len() < 18 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(bytes[..4].try_into()?);
        let topic_id = u32::from_le_bytes(bytes[4..8].try_into()?);
        let key_kind = KeyKind::from_code(bytes[8])?;
        let key_value = u32::from_le_bytes(bytes[9..13].try_into()?);
        let messages_count = u32::from_le_bytes(bytes[13..17].try_into()?);
        let messages_payloads = &bytes[17..];
        let mut position = 0;
        let mut messages = Vec::with_capacity(messages_count as usize);
        while position < messages_payloads.len() {
            let message = Message::from_bytes(&messages_payloads[position..])?;
            position += message.get_size_bytes() as usize;
            messages.push(message);
        }

        let command = SendMessages {
            stream_id,
            topic_id,
            key_kind,
            key_value,
            messages_count,
            messages,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for SendMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}",
            self.stream_id,
            self.topic_id,
            self.key_kind,
            self.key_value,
            self.messages
                .iter()
                .map(|message| message.to_string())
                .collect::<Vec<String>>()
                .join("|")
        )
    }
}

impl Display for KeyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyKind::PartitionId => write!(f, "partition_id"),
            KeyKind::EntityId => write!(f, "entity_id"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let message_1 = Message::from_str("hello 1").unwrap();
        let message_2 = Message::from_str("2|hello 2").unwrap();
        let message_3 = Message::from_str("3|hello 3").unwrap();
        let messages = vec![message_1, message_2, message_3];
        let command = SendMessages {
            stream_id: 1,
            topic_id: 2,
            key_kind: KeyKind::PartitionId,
            key_value: 4,
            messages_count: messages.len() as u32,
            messages,
        };

        let bytes = command.as_bytes();
        let stream_id = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        let topic_id = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let key_kind = KeyKind::from_code(bytes[8]).unwrap();
        let key_value = u32::from_le_bytes(bytes[9..13].try_into().unwrap());
        let messages_count = u32::from_le_bytes(bytes[13..17].try_into().unwrap());
        let messages = &bytes[17..];
        let command_messages = &command
            .messages
            .iter()
            .map(|message| message.as_bytes())
            .collect::<Vec<Vec<u8>>>()
            .concat();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(key_kind, command.key_kind);
        assert_eq!(key_value, command.key_value);
        assert_eq!(messages_count, command.messages_count);
        assert_eq!(messages, command_messages);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = 1u32;
        let topic_id = 2u32;
        let key_kind = KeyKind::PartitionId;
        let key_value = 4u32;
        let messages_count = 3u32;

        let message_1 = Message::from_str("hello 1").unwrap();
        let message_2 = Message::from_str("2|hello 2").unwrap();
        let message_3 = Message::from_str("3|hello 3").unwrap();
        let messages = vec![
            message_1.as_bytes(),
            message_2.as_bytes(),
            message_3.as_bytes(),
        ]
        .concat();

        let mut bytes: Vec<u8> = [stream_id.to_le_bytes(), topic_id.to_le_bytes()].concat();

        bytes.extend(key_kind.as_code().to_le_bytes());
        bytes.extend(key_value.to_le_bytes());
        bytes.extend(messages_count.to_le_bytes());
        bytes.extend(messages);

        let command = SendMessages::from_bytes(&bytes);
        assert!(command.is_ok());

        let messages_payloads = &bytes[17..];
        let mut position = 0;
        let mut messages = Vec::with_capacity(messages_count as usize);
        while position < messages_payloads.len() {
            let message = Message::from_bytes(&messages_payloads[position..]).unwrap();
            position += message.get_size_bytes() as usize;
            messages.push(message);
        }

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.key_kind, key_kind);
        assert_eq!(command.key_value, key_value);
        assert_eq!(command.messages_count, messages_count);
        for i in 0..command.messages_count {
            let message = &messages[i as usize];
            let command_message = &command.messages[i as usize];
            assert_eq!(command_message.id, message.id);
            assert_eq!(command_message.length, message.length);
            assert_eq!(command_message.payload, message.payload);
        }
    }

    // For now, we only support a single payload.
    #[test]
    fn should_be_read_from_string() {
        let stream_id = 1u32;
        let topic_id = 2u32;
        let key_kind = KeyKind::PartitionId;
        let key_value = 4u32;
        let messages_count = 1u32;
        let message_id = 1u128;
        let payload = "hello";
        let input = format!(
            "{}|{}|{}|{}|{}|{}",
            stream_id, topic_id, key_kind, key_value, message_id, payload
        );

        let command = SendMessages::from_str(&input);

        assert!(command.is_ok());
        let command = command.unwrap();
        let message = &command.messages[0];
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.key_kind, key_kind);
        assert_eq!(command.key_value, key_value);
        assert_eq!(command.messages_count, messages_count);
        assert_eq!(message.id, message_id);
        assert_eq!(message.length, payload.len() as u32);
        assert_eq!(message.payload, payload.as_bytes());
    }
}
