use super::message_batch::RetainedMessageBatch;
use crate::streaming::models::messages::RetainedMessage;
use std::sync::Arc;

pub trait IntoMessagesIterator {
    type Item;
    type IntoIter: Iterator<Item = Self::Item>;
    fn into_messages_iter(self) -> Self::IntoIter;
}

pub struct RetainedMessageBatchIterator<'a> {
    batch: &'a RetainedMessageBatch,
    current_position: u32,
}

impl<'a> RetainedMessageBatchIterator<'a> {
    pub fn new(batch: &'a RetainedMessageBatch) -> Self {
        RetainedMessageBatchIterator {
            batch,
            current_position: 0,
        }
    }
}

// TODO(numinex): Consider using FallibleIterator instead of this
// https://crates.io/crates/fallible-iterator
impl<'a> Iterator for RetainedMessageBatchIterator<'a> {
    type Item = RetainedMessage;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_position < self.batch.length {
            let start_position = self.current_position as usize;
            let length = u32::from_le_bytes(
                self.batch.bytes[start_position..start_position + 4]
                    .try_into()
                    .ok()?,
            );
            let message = self
                .batch
                .bytes
                .slice(start_position + 4..start_position + 4 + length as usize);
            self.current_position += 4 + length;
            RetainedMessage::try_from_bytes(message).ok()
        } else {
            None
        }
    }
}

impl<'a> IntoMessagesIterator for &'a RetainedMessageBatch {
    type Item = RetainedMessage;
    type IntoIter = RetainedMessageBatchIterator<'a>;

    fn into_messages_iter(self) -> Self::IntoIter {
        RetainedMessageBatchIterator::new(self)
    }
}
impl<'a> IntoMessagesIterator for &'a Arc<RetainedMessageBatch> {
    type Item = RetainedMessage;
    type IntoIter = RetainedMessageBatchIterator<'a>;

    fn into_messages_iter(self) -> Self::IntoIter {
        RetainedMessageBatchIterator::new(self)
    }
}
