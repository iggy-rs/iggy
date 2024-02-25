use std::sync::Arc;

use crate::streaming::batching::batch_filter::BatchFilter;
use crate::streaming::batching::iterator::IntoBatchIterator;
use crate::streaming::models::messages::RetainedMessage;
use bytes::{BufMut, Bytes, BytesMut};
use iggy::error::IggyError::{
    self, MissingBaseOffsetRetainedMessageBatch, MissingLastOffsetDeltaRetainedMessageBatch,
    MissingLengthRetainedMessageBatch, MissingMaxTimestampRetainedMessageBatch,
    MissingPayloadRetainedMessageBatch,
};

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

    pub fn builder() -> RetainedMessageBatchBuilder {
        RetainedMessageBatchBuilder::new()
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

    pub fn extend(&self, bytes: &mut BytesMut) {
        bytes.put_u64_le(self.base_offset);
        bytes.put_u32_le(self.length);
        bytes.put_u32_le(self.last_offset_delta);
        bytes.put_u64_le(self.max_timestamp);
        bytes.put_slice(&self.bytes);
    }
}

impl<'a, T, U> BatchFilter<RetainedMessage, &'a U, T> for T
where
    T: Iterator<Item = &'a U>,
    &'a U: IntoBatchIterator<Item = RetainedMessage>,
{
    fn convert_and_filter_by_offset_range(
        self,
        start_offset: u64,
        end_offset: u64,
    ) -> Vec<RetainedMessage> {
        let messages_size = start_offset + end_offset;
        self.fold(
            Vec::with_capacity(messages_size as usize),
            |mut messages, batch| {
                messages.extend(
                    batch
                        .into_messages_iter()
                        .filter(|msg| msg.offset >= start_offset && msg.offset <= end_offset),
                );
                messages
            },
        )
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
