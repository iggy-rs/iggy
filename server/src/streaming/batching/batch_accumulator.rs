use super::message_batch::{RetainedMessageBatch, RETAINED_BATCH_OVERHEAD};
use crate::streaming::local_sizeable::LocalSizeable;
use crate::streaming::models::messages::RetainedMessage;
use bytes::BytesMut;
use iggy::models::batch::IggyBatch;
use iggy::models::messages::{ArchivedIggyMessage, IggyMessage};
use iggy::utils::sizeable::Sizeable;
use iggy::{models::batch::ArchivedIggyBatch, utils::byte_size::IggyByteSize};
use rkyv::de::Pool;
use rkyv::rancor::{Fallible, Strategy};
use rkyv::util::{Align, AlignedVec};
use rkyv::vec::ArchivedVec;
use rkyv::{access, Deserialize};
use std::sync::Arc;

#[derive(Debug)]
pub struct BatchAccumulator {
    base_offset: u64,
    current_size: IggyByteSize,
    current_offset: u64,
    current_timestamp: u64,
    capacity: u64,
    batches: Vec<AlignedVec<512>>,
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

    pub fn append(&mut self, batch_size: IggyByteSize, batch: AlignedVec<512>) {
        let access_batch = rkyv::access::<ArchivedIggyBatch, rkyv::rancor::Error>(&batch).unwrap();
        let batch_base_offset = access_batch.base_offset.to_native();
        let batch_base_timestamp = access_batch.base_timestamp.to_native();
        self.current_size += batch_size;
        self.current_offset = access_batch
            .messages
            .last()
            .unwrap()
            .offset_delta
            .to_native() as u64
            + batch_base_offset;
        self.current_timestamp = access_batch
            .messages
            .last()
            .unwrap()
            .timestamp_delta
            .to_native() as u64
            + batch_base_timestamp;
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

    pub fn materialize_batch_and_maybe_update_state(&mut self) -> AlignedVec {
        let mut capacity = 0;
        for batch in &self.batches {
            let access_batch =
                rkyv::access::<ArchivedIggyBatch, rkyv::rancor::Error>(batch).unwrap();
            capacity += access_batch.messages.len();
        }

        let mut messages = Vec::with_capacity(capacity);
        for batch in &self.batches {
            let archived_batch =
                rkyv::access::<ArchivedIggyBatch, rkyv::rancor::Error>(&batch).unwrap();
            let batch =
                rkyv::deserialize::<IggyBatch, rkyv::rancor::Error>(archived_batch).unwrap();
            messages.extend(batch.messages);
        }
        let batch = IggyBatch::accumulated(self.base_offset, self.current_timestamp, messages);
        rkyv::to_bytes::<rkyv::rancor::Error>(&batch).unwrap()
    }
}

impl Sizeable for BatchAccumulator {
    fn get_size_bytes(&self) -> IggyByteSize {
        self.current_size + RETAINED_BATCH_OVERHEAD.into()
    }
}
