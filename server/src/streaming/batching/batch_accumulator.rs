use iggy::models::batch::{IggyBatch, IggyHeader};
use iggy::utils::byte_size::IggyByteSize;
use std::sync::Arc;

#[derive(Debug)]
pub struct BatchAccumulator {
    base_offset: u64,
    current_size: IggyByteSize,
    current_offset: u64,
    current_timestamp: u64,
    leading_header: Option<IggyHeader>,
    batches: Vec<IggyBatch>,
}

impl BatchAccumulator {
    pub fn new(base_offset: u64, capacity: usize) -> Self {
        Self {
            base_offset,
            current_size: IggyByteSize::from(0),
            current_offset: 0,
            current_timestamp: 0,
            leading_header: None,
            batches: Vec::with_capacity(capacity),
        }
    }

    pub fn append(&mut self, batch_size: IggyByteSize, batch: IggyBatch) {
        let header = batch.header;
        if self.leading_header.is_none() {
            self.base_offset = header.base_offset;
            self.leading_header = Some(header);
        }
        self.current_timestamp = header.base_timestamp + header.last_timestamp_delta as u64;
        self.current_offset = header.base_offset + header.last_offset_delta as u64;
        self.current_size += batch_size;
        self.batches.push(batch);
        //TODO: Fix me
    }

    pub fn get_messages_by_offset(&self, start_offset: u64, end_offset: u64) -> Vec<Arc<()>> {
        //TODO: Fix me
        /*
        let start_idx = self
            .messages
            .partition_point(|msg| msg.offset < start_offset);
        let end_idx = self
            .messages
            .partition_point(|msg| msg.offset <= end_offset);
        self.messages[start_idx..end_idx].to_vec()
        */
        todo!()
    }

    pub fn get_messages_by_timestamp(&self, start_timestamp: u64, count: usize) -> Vec<Arc<()>> {
        //TODO: Fix me
        /*
        let start_idx = self
            .messages
            .partition_point(|msg| msg.timestamp < start_timestamp);
        let end_idx = std::cmp::min(start_idx + count, self.messages.len());
        self.messages[start_idx..end_idx].to_vec()
        */
        todo!()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn unsaved_messages_count(&self) -> usize {
        //TODO: Fix me, replace this with leading_header last_offset_delta + 1.
        self.batches
            .iter()
            .map(|b| b.header)
            .map(|h| (h.last_offset_delta + 1) as usize)
            .sum()
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

    pub fn materialize(self) -> (IggyHeader, Vec<IggyBatch>) {
        (
            self.leading_header
                .expect("No leading header when materializing batch"),
            self.batches,
        )
    }
}
