use super::message_batch::{RetainedMessageBatch, RETAINED_BATCH_OVERHEAD};
use crate::streaming::{models::messages::RetainedMessage, sizeable::Sizeable};
use bytes::BytesMut;
use std::sync::Arc;

#[derive(Debug)]
pub struct BatchAccumulator {
    base_offset: u64,
    current_size: u64,
    current_offset: u64,
    current_timestamp: u64,
    capacity: u64,
    messages: Vec<Arc<RetainedMessage>>,
}

impl BatchAccumulator {
    pub fn new(base_offset: u64, capacity: usize) -> Self {
        Self {
            base_offset,
            current_size: 0,
            current_offset: 0,
            current_timestamp: 0,
            capacity: capacity as u64,
            messages: Vec::with_capacity(capacity),
        }
    }

    pub fn append(&mut self, batch_size: u64, items: &[Arc<RetainedMessage>]) {
        assert!(!items.is_empty());
        self.current_size += batch_size;
        self.current_offset = items.last().unwrap().offset;
        self.current_timestamp = items.last().unwrap().timestamp;
        self.messages.extend(items.iter().cloned());
    }

    pub fn get_messages_by_offset(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Vec<Arc<RetainedMessage>> {
        self.messages
            .iter()
            .filter(|msg| msg.offset >= start_offset && msg.offset <= end_offset)
            .cloned()
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn unsaved_messages_count(&self) -> usize {
        self.messages.len()
    }

    pub fn batch_max_offset(&self) -> u64 {
        self.current_offset
    }

    pub fn batch_max_timestamp(&self) -> u64 {
        self.current_timestamp
    }

    pub fn batch_base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn get_size_bytes(&self) -> u64 {
        self.current_size + RETAINED_BATCH_OVERHEAD as u64
    }

    pub fn materialize_batch_and_maybe_update_state(&mut self) -> (bool, RetainedMessageBatch) {
        let batch_base_offset = self.base_offset;
        let batch_last_offset_delta = (self.current_offset - self.base_offset) as u32;
        let batch_max_timestamp = self.messages.last().unwrap().timestamp;
        let split_point = std::cmp::min(self.capacity as usize, self.messages.len());
        let (batch, remainder) = self.messages.as_slice().split_at(split_point);
        let mut bytes = BytesMut::with_capacity(self.current_size as usize);
        for message in batch {
            message.extend(&mut bytes);
        }

        let mut remaining_messages = Vec::with_capacity(remainder.len());
        let has_remainder = !remainder.is_empty();
        if has_remainder {
            self.base_offset = remainder.first().unwrap().offset;
            self.current_size = remainder
                .iter()
                .map(|msg| msg.get_size_bytes() as u64)
                .sum();
            self.current_offset = remainder.last().unwrap().offset;
            self.current_timestamp = remainder.last().unwrap().timestamp;
            for message in remainder {
                remaining_messages.push(message.clone());
            }
            self.messages = remaining_messages;
        }
        let batch_payload = bytes.freeze();
        let batch_payload_len = batch_payload.len() as u32;
        let batch = RetainedMessageBatch::new(
            batch_base_offset,
            batch_last_offset_delta,
            batch_max_timestamp,
            batch_payload_len,
            batch_payload,
        );
        (has_remainder, batch)
    }
}
