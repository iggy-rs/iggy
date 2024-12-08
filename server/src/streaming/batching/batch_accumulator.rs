use super::message_batch::{RetainedMessageBatch, RETAINED_BATCH_OVERHEAD};
use crate::streaming::local_sizeable::LocalSizeable;
use crate::streaming::models::messages::RetainedMessage;
use bytes::BytesMut;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::sizeable::Sizeable;
use std::sync::Arc;

#[derive(Debug)]
pub struct BatchAccumulator {
    base_offset: u64,
    current_size: IggyByteSize,
    current_offset: u64,
    current_timestamp: u64,
    capacity: u64,
    messages: Vec<Arc<RetainedMessage>>,
}

impl BatchAccumulator {
    pub fn new(base_offset: u64, capacity: usize) -> Self {
        Self {
            base_offset,
            current_size: IggyByteSize::from(0),
            current_offset: 0,
            current_timestamp: 0,
            capacity: capacity as u64,
            messages: Vec::with_capacity(capacity),
        }
    }

    pub fn append(&mut self, batch_size: IggyByteSize, items: &[Arc<RetainedMessage>]) {
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

    pub fn materialize_batch_and_maybe_update_state(&mut self) -> (bool, RetainedMessageBatch) {
        let batch_base_offset = self.base_offset;
        let batch_last_offset_delta = (self.current_offset - self.base_offset) as u32;
        let split_point = std::cmp::min(self.capacity as usize, self.messages.len());

        let mut bytes = BytesMut::with_capacity(self.current_size.as_bytes_u64() as usize);
        let last_batch_timestamp = self
            .messages
            .get(split_point - 1)
            .map_or(0, |msg| msg.timestamp);
        for message in self.messages.drain(..split_point) {
            message.extend(&mut bytes);
        }

        let has_remainder = !self.messages.is_empty();
        if has_remainder {
            self.base_offset = self.messages.first().unwrap().offset;
            self.current_size = self
                .messages
                .iter()
                .map(|msg| msg.get_size_bytes())
                .sum::<IggyByteSize>();
            self.current_offset = self.messages.last().unwrap().offset;
            self.current_timestamp = self.messages.last().unwrap().timestamp;
        }

        let batch_payload = bytes.freeze();
        let batch_payload_len = IggyByteSize::from(batch_payload.len() as u64);
        let batch = RetainedMessageBatch::new(
            batch_base_offset,
            batch_last_offset_delta,
            last_batch_timestamp,
            batch_payload_len,
            batch_payload,
        );
        (has_remainder, batch)
    }
}

impl Sizeable for BatchAccumulator {
    fn get_size_bytes(&self) -> IggyByteSize {
        self.current_size + RETAINED_BATCH_OVERHEAD.into()
    }
}
