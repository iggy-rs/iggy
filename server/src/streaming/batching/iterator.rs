use super::message_batch::RetainedMessageBatch;
use iggy::models::messages::PolledMessage;
use std::sync::Arc;

pub struct RetainedMessageBatchIterator {
    batch: Arc<RetainedMessageBatch>,
    current_position: u32,
}

impl RetainedMessageBatchIterator {
    pub fn new(batch: Arc<RetainedMessageBatch>) -> Self {
        RetainedMessageBatchIterator {
            batch,
            current_position: 0,
        }
    }
}

impl Iterator for RetainedMessageBatchIterator {
    type Item = PolledMessage;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_position < self.batch.length {
            let start_position = (self.current_position) as usize;
            let message_bytes = self.batch.bytes.slice(start_position..);
            self.current_position += 1;
            PolledMessage::try_from_bytes(message_bytes).ok()
        } else {
            None
        }
    }
}

impl IntoIterator for RetainedMessageBatch {
    type Item = PolledMessage;
    type IntoIter = RetainedMessageBatchIterator;

    fn into_iter(self) -> Self::IntoIter {
        RetainedMessageBatchIterator::new(&self)
    }
}
