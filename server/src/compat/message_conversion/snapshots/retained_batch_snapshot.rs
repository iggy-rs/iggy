use super::message_snapshot::MessageSnapshot;
use crate::compat::message_conversion::message_converter::Extendable;
use crate::server_error::ServerError;
use crate::streaming::sizeable::Sizeable;
use bytes::{BufMut, Bytes, BytesMut};
use iggy::error::IggyError;

pub struct RetainedMessageBatchSnapshot {
    pub base_offset: u64,
    pub last_offset_delta: u32,
    pub max_timestamp: u64,
    pub length: u32,
    pub bytes: Bytes,
}

impl RetainedMessageBatchSnapshot {
    pub fn new(
        base_offset: u64,
        last_offset_delta: u32,
        max_timestamp: u64,
        length: u32,
        bytes: Bytes,
    ) -> RetainedMessageBatchSnapshot {
        RetainedMessageBatchSnapshot {
            base_offset,
            last_offset_delta,
            max_timestamp,
            length,
            bytes,
        }
    }

    pub fn get_last_offset(&self) -> u64 {
        self.base_offset + self.last_offset_delta as u64
    }

    pub fn try_from_messages(
        messages: Vec<MessageSnapshot>,
    ) -> Result<RetainedMessageBatchSnapshot, IggyError> {
        let first_message = messages.first().unwrap();
        let last_message = messages.last().unwrap();
        let base_offset = first_message.offset;
        let last_offset_delta = last_message.offset - base_offset;
        let max_timestamp = last_message.timestamp;

        let size = messages
            .iter()
            .map(|msg| msg.get_size_bytes() as usize)
            .sum();
        let mut bytes = BytesMut::with_capacity(size);
        for message in messages.iter() {
            message.extend(&mut bytes);
        }
        Ok(RetainedMessageBatchSnapshot::new(
            base_offset,
            last_offset_delta as u32,
            max_timestamp,
            bytes.len() as u32,
            bytes.freeze(),
        ))
    }
}
impl Sizeable for RetainedMessageBatchSnapshot {
    fn get_size_bytes(&self) -> u32 {
        24 + self.bytes.len() as u32
    }
}

impl Extendable for RetainedMessageBatchSnapshot {
    fn extend(&self, bytes: &mut BytesMut) {
        bytes.put_u64_le(self.base_offset);
        bytes.put_u32_le(self.length);
        bytes.put_u32_le(self.last_offset_delta);
        bytes.put_u64_le(self.max_timestamp);
        bytes.put_slice(&self.bytes);
    }
}

impl TryFrom<Bytes> for RetainedMessageBatchSnapshot {
    type Error = ServerError;
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let base_offset = u64::from_le_bytes(
            value
                .get(0..8)
                .ok_or_else(|| {
                    ServerError::CannotReadMessageBatchFormatConversion(
                        "Failed to read batch base offset".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let length = u32::from_le_bytes(
            value
                .get(8..12)
                .ok_or_else(|| {
                    ServerError::CannotReadMessageBatchFormatConversion(
                        "Failed to read batch length".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let last_offset_delta = u32::from_le_bytes(
            value
                .get(12..16)
                .ok_or_else(|| {
                    ServerError::CannotReadMessageBatchFormatConversion(
                        "Failed to read batch last_offset_delta".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let max_timestamp = u64::from_le_bytes(
            value
                .get(16..24)
                .ok_or_else(|| {
                    ServerError::CannotReadMessageBatchFormatConversion(
                        "Failed to read batch max_timestamp".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let bytes = Bytes::from(
            value
                .get(24..length as usize)
                .ok_or_else(|| {
                    ServerError::CannotReadMessageBatchFormatConversion(
                        "Failed to read batch payload".to_owned(),
                    )
                })?
                .to_owned(),
        );
        Ok(RetainedMessageBatchSnapshot {
            base_offset,
            last_offset_delta,
            max_timestamp,
            length,
            bytes,
        })
    }
}
