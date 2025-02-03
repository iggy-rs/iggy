use super::message_batch::{RetainedMessageBatch, RETAINED_BATCH_OVERHEAD};
use crate::streaming::local_sizeable::LocalSizeable;
use crate::streaming::models::messages::RetainedMessage;
use bytes::BytesMut;
use iggy::models::batch::IggyBatch;
use iggy::models::messages::{ArchivedIggyMessage, IggyMessage};
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::sizeable::Sizeable;
use rkyv::de::Pool;
use rkyv::rancor::{Fallible, Strategy};
use rkyv::util::{Align, AlignedVec};
use rkyv::vec::ArchivedVec;
use rkyv::{access, Deserialize};
use std::sync::Arc;
use tracing::error;

#[derive(Debug)]
pub struct BatchAccumulator {
    base_offset: u64,
    pub current_size: IggyByteSize,
    current_offset: u64,
    current_timestamp: u64,
    capacity: u64,
    pub batches: Vec<IggyBatch>,
}

impl BatchAccumulator {
    pub fn new(base_offset: u64, capacity: usize) -> Self {
        Self {
            base_offset,
            current_size: IggyByteSize::from(0),
            current_offset: 0,
            current_timestamp: 0,
            capacity: capacity as u64,
            batches: Vec::with_capacity(capacity),
        }
    }

    pub fn append(&mut self, batch_size: IggyByteSize, batch: IggyBatch) {
        let batch_base_offset = batch.header.base_offset;
        let batch_base_timestamp = batch.header.base_timestamp;
        self.current_size += (batch.messages.len() as u64).into();
        self.current_offset = batch.header.base_offset + batch.header.last_offset_delta as u64;
        self.batches.push(batch);
    }

    pub fn get_messages_by_offset(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Vec<Arc<RetainedMessage>> {
        todo!()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn unsaved_messages_count(&self) -> usize {
        self.batches.len()
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
}

impl Sizeable for BatchAccumulator {
    fn get_size_bytes(&self) -> IggyByteSize {
        self.current_size + RETAINED_BATCH_OVERHEAD.into()
    }
}
