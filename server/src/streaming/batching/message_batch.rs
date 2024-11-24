use crate::streaming::batching::batch_filter::BatchItemizer;
use crate::streaming::batching::iterator::IntoMessagesIterator;
use crate::streaming::models::messages::RetainedMessage;
use bytes::{BufMut, Bytes, BytesMut};
use tracing::warn;

pub const RETAINED_BATCH_OVERHEAD: u32 = 8 + 8 + 4 + 4;

use crate::streaming::sizeable::Sizeable;
#[derive(Debug, Clone)]
pub struct RetainedMessageBatch {
    pub base_offset: u64,
    pub last_offset_delta: u32,
    pub max_timestamp: u64,
    pub length: u32,
    pub bytes: Bytes,
}

impl RetainedMessageBatch {
    pub fn new(
        base_offset: u64,
        last_offset_delta: u32,
        max_timestamp: u64,
        length: u32,
        bytes: Bytes,
    ) -> Self {
        RetainedMessageBatch {
            base_offset,
            last_offset_delta,
            max_timestamp,
            length,
            bytes,
        }
    }

    pub fn is_contained_or_overlapping_within_offset_range(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> bool {
        (self.base_offset <= end_offset && self.get_last_offset() >= end_offset)
            || (self.base_offset <= start_offset && self.get_last_offset() <= end_offset)
            || (self.base_offset <= end_offset && self.get_last_offset() >= start_offset)
    }

    pub fn get_last_offset(&self) -> u64 {
        self.base_offset + self.last_offset_delta as u64
    }

    pub fn extend2(&self, bytes: &mut Vec<u8>) {
        bytes[0..8].copy_from_slice(&self.base_offset.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.length.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.last_offset_delta.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.max_timestamp.to_le_bytes());
        bytes[24..self.length as usize + RETAINED_BATCH_OVERHEAD as usize]
            .copy_from_slice(&self.bytes);
    }

    pub fn extend(&self, bytes: &mut BytesMut) {
        bytes.put_u64_le(self.base_offset);
        bytes.put_u32_le(self.length);
        bytes.put_u32_le(self.last_offset_delta);
        bytes.put_u64_le(self.max_timestamp);
        bytes.put_slice(&self.bytes);
    }
}

impl<'a, T, U> BatchItemizer<RetainedMessage, &'a U, T> for T
where
    T: Iterator<Item = &'a U>,
    &'a U: IntoMessagesIterator<Item = RetainedMessage>,
{
    fn to_messages(self) -> Vec<RetainedMessage> {
        self.flat_map(|batch| batch.into_messages_iter().collect::<Vec<_>>())
            .collect()
    }

    fn to_messages_with_filter<F>(self, messages_count: usize, f: &F) -> Vec<RetainedMessage>
    where
        F: Fn(&RetainedMessage) -> bool,
    {
        self.fold(Vec::with_capacity(messages_count), |mut messages, batch| {
            messages.extend(batch.into_messages_iter().filter(f));
            messages
        })
    }
}

impl Sizeable for RetainedMessageBatch {
    fn get_size_bytes(&self) -> u32 {
        RETAINED_BATCH_OVERHEAD + self.length
    }
}
