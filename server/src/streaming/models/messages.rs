use crate::streaming::sizeable::Sizeable;
use bytes::{BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::models::messages::PolledMessage;
use iggy::utils::checksum;
use iggy::{messages::send_messages::Message, models::messages::MessageState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

// It's the same as PolledMessages from Iggy models, but with the Arc<Message> instead of Message.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledMessages {
    pub partition_id: u32,
    pub current_offset: u64,
    pub messages: Vec<Rc<PolledMessage>>,
}

#[derive(Debug)]
pub struct RetainedMessage {
    pub id: u128,
    pub offset: u64,
    pub timestamp: u64,
    pub checksum: u32,
    pub message_state: MessageState,
    pub headers: Option<Bytes>,
    pub payload: Bytes,
}

impl TryFrom<RetainedMessage> for PolledMessage {
    type Error = IggyError;
    fn try_from(value: RetainedMessage) -> Result<Self, Self::Error> {
        let headers = value.headers.map(HashMap::from_bytes).transpose()?;
        let messages = PolledMessage {
            offset: value.offset,
            state: value.message_state,
            timestamp: value.timestamp,
            id: value.id,
            checksum: value.checksum,
            headers,
            length: value.payload.len() as u32,
            payload: value.payload,
        };
        Ok(messages)
    }
}

impl RetainedMessage {
    pub fn new(offset: u64, timestamp: u64, message: Message) -> Self {
        RetainedMessage {
            offset,
            timestamp,
            checksum: checksum::calculate(&message.payload),
            message_state: MessageState::Available,
            id: message.id,
            payload: message.payload.clone(),
            headers: message.headers.as_ref().map(|headers| headers.as_bytes()),
        }
    }

    pub fn extend(&self, bytes: &mut BytesMut) {
        let length = self.get_size_bytes();
        let id = self.id;
        let offset = self.offset;
        let timestamp = self.timestamp;
        let payload = self.payload.clone();
        let checksum = self.checksum;
        let message_state = self.message_state;
        let headers = &self.headers;

        bytes.put_u32_le(length);
        bytes.put_u64_le(offset);
        bytes.put_u8(message_state.as_code());
        bytes.put_u64_le(timestamp);
        bytes.put_u128_le(id);
        bytes.put_u32_le(checksum);
        if let Some(headers) = headers {
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u32_le(headers.len() as u32);
            bytes.put_slice(headers);
        } else {
            bytes.put_u32_le(0u32);
        }
        bytes.put_slice(&payload);
    }

    pub fn try_from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        let offset = u64::from_le_bytes(bytes[..8].try_into()?);
        let message_state = MessageState::from_code(bytes[8])?;
        let timestamp = u64::from_le_bytes(bytes[9..17].try_into()?);
        let id = u128::from_le_bytes(bytes[17..33].try_into()?);
        let checksum = u32::from_le_bytes(bytes[33..37].try_into()?);
        let headers_length = u32::from_le_bytes(bytes[37..41].try_into()?);
        let headers = if headers_length > 0 {
            Some(bytes.slice(41..41 + headers_length as usize))
        } else {
            None
        };
        let position = 41 + headers_length as usize;
        let payload = bytes.slice(position..);

        Ok(RetainedMessage {
            id,
            offset,
            timestamp,
            checksum,
            message_state,
            headers,
            payload,
        })
    }
}

impl Sizeable for RetainedMessage {
    fn get_size_bytes(&self) -> u32 {
        let headers_len = self
            .headers
            .as_ref()
            .map(|h| 4 + h.len() as u32)
            .unwrap_or(4);
        16 + 8 + 8 + 4 + 1 + headers_len + self.payload.len() as u32
    }
}
