use bytes::{BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::error::IggyError::{
    MissingBaseOffsetRetainedMessageBatch, MissingLastOffsetDeltaRetainedMessageBatch,
    MissingLengthRetainedMessageBatch, MissingMaxTimestampRetainedMessageBatch,
    MissingPayloadRetainedMessageBatch,
};
use iggy::models::messages::PolledMessage;
use iggy::utils::checksum;
use iggy::{messages::send_messages::Message, models::messages::MessageState};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::streaming::sizeable::Sizeable;

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
    pub message: &'a Message,
}

impl<'a> RetainedMessage<'a> {
    pub fn new(offset: u64, timestamp: u64, message: &'a Message) -> Self {
        RetainedMessage {
            offset,
            timestamp,
            message,
        }
    }
    pub fn extend(&self, bytes: &mut BytesMut) {
        let id = self.message.id;
        let offset = self.offset;
        let timestamp = self.timestamp;
        let length = self.message.length;
        let payload = &self.message.payload;
        let checksum = checksum::calculate(payload);
        let message_state = MessageState::Available;
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
}

impl<'a> Sizeable for RetainedMessage<'a> {
    fn get_size_bytes(&self) -> u32 {
        8 + 8 + self.message.length
    }
}

#[derive(Debug, Clone)]
pub struct RetainedMessageBatch {
    pub base_offset: u64,
    pub last_offset_delta: u32,
    pub max_timestamp: u64,
    pub length: u32,
    pub bytes: Bytes,
}

impl RetainedMessageBatch {
    pub fn builder() -> RetainedMessageBatchBuilder {
        RetainedMessageBatchBuilder::new()
    }
}

impl Sizeable for RetainedMessageBatch {
    fn get_size_bytes(&self) -> u32 {
        8 + 4 + 8 + 4 + self.length
    }
}

impl Sizeable for Arc<RetainedMessageBatch> {
    fn get_size_bytes(&self) -> u32 {
        8 + 4 + 8 + 4 + self.length
    }
}

#[derive(Debug, Clone)]
pub struct RetainedMessageBatchBuilder {
    base_offset: Option<u64>,
    last_offset_delta: Option<u32>,
    max_timestamp: Option<u64>,
    length: Option<u32>,
    payload: Option<Bytes>,
}

impl RetainedMessageBatchBuilder {
    fn new() -> Self {
        RetainedMessageBatchBuilder {
            base_offset: None,
            last_offset_delta: None,
            max_timestamp: None,
            length: None,
            payload: None,
        }
    }

    pub fn base_offset(mut self, base_offset: u64) -> Self {
        self.base_offset = Some(base_offset);
        self
    }

    pub fn last_offset_delta(mut self, last_offset_delta: u32) -> Self {
        self.last_offset_delta = Some(last_offset_delta);
        self
    }

    pub fn max_timestamp(mut self, max_timestamp: u64) -> Self {
        self.max_timestamp = Some(max_timestamp);
        self
    }

    pub fn length(mut self, length: u32) -> Self {
        self.length = Some(length);
        self
    }

    pub fn payload(mut self, payload: Bytes) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn build(self) -> Result<RetainedMessageBatch, IggyError> {
        let base_offset = self
            .base_offset
            .ok_or(MissingBaseOffsetRetainedMessageBatch)?;
        let last_offset_delta = self
            .last_offset_delta
            .ok_or(MissingLastOffsetDeltaRetainedMessageBatch)?;
        let max_timestamp = self
            .max_timestamp
            .ok_or(MissingMaxTimestampRetainedMessageBatch)?;
        let length = self.length.ok_or(MissingLengthRetainedMessageBatch)?;
        let bytes = self.payload.ok_or(MissingPayloadRetainedMessageBatch)?;

        Ok(RetainedMessageBatch {
            base_offset,
            last_offset_delta,
            max_timestamp,
            length,
            bytes,
        })
    }
}
