use crate::streaming::sizeable::Sizeable;
use bytes::{BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::models::messages::PolledMessage;
use iggy::utils::checksum;
use iggy::{messages::send_messages::Message, models::messages::MessageState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// It's the same as PolledMessages from Iggy models, but with the Arc<Message> instead of Message.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledMessages {
    pub partition_id: u32,
    pub current_offset: u64,
    pub messages: Vec<Arc<PolledMessage>>,
}

pub struct RetainedMessage<'a> {
    pub offset: u64,
    pub timestamp: u64,
    pub checksum: u32,
    pub message_state: MessageState,
    pub message: &'a Message,
}

impl<'a> RetainedMessage<'a> {
    pub fn new(offset: u64, timestamp: u64, message: &Message) -> Self {
        RetainedMessage {
            offset,
            timestamp,
            checksum: checksum::calculate(&message.payload),
            message_state: MessageState::Available,
            message,
        }
    }

    pub fn extend(&self, bytes: &mut BytesMut) {
        let id = self.message.id;
        let offset = self.offset;
        let timestamp = self.timestamp;
        let length = self.message.length;
        let payload = &self.message.payload;
        let checksum = self.checksum;
        let message_state = self.message_state;
        let headers = self.message.headers.as_ref().cloned();

        bytes.put_u64_le(offset);
        bytes.put_u8(message_state.as_code());
        bytes.put_u64_le(timestamp);
        bytes.put_u128_le(id);
        bytes.put_u32_le(checksum);
        if let Some(headers) = headers {
            let headers_bytes = headers.as_bytes();
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u32_le(headers_bytes.len() as u32);
            bytes.put_slice(&headers_bytes);
        } else {
            bytes.put_u32_le(0u32);
        }
        bytes.put_u32_le(length);
        bytes.put_slice(payload);
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, IggyError> {
        let offset = u64::from_le_bytes(bytes[..8].try_into()?);
        let message_state = MessageState::from_code(bytes[8])?;
        let timestamp = u64::from_le_bytes(bytes[9..17].try_into()?);
        let id = u128::from_le_bytes(bytes[17..33].try_into()?);
        let checksum = u32::from_le_bytes(bytes[33..37].try_into()?);
        let headers_length = u32::from_le_bytes(bytes[37..41].try_into()?);
        let headers = if headers_length > 0 {
            Some(HashMap::from_bytes(Bytes::from(
                &bytes[41..headers_length as usize],
            ))?)
        } else {
            None
        };
        let position = 41 + headers_length as usize;
        let length = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let payload = Bytes::from(&bytes[position + 4..position + 4 + length as usize]);

        Ok(RetainedMessage {
            offset,
            timestamp,
            checksum,
            message_state,
            message: &Message {
                id,
                length,
                payload,
                headers,
            },
        })
    }
}

impl<'a> Sizeable for RetainedMessage<'a> {
    fn get_size_bytes(&self) -> u32 {
        8 + 8 + 4 + 1 + self.message.length
    }
}
