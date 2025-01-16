use super::messages::IggyMessage;
use crate::{
    messages::send_messages::Message,
    utils::{byte_size::IggyByteSize, sizeable::Sizeable},
};
use serde_with::serde_as;

const IGGY_BATCH_OVERHEAD: u64 = 16 + 16 + 16 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + 1;

#[serde_as]
#[derive(
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[rkyv(derive(Debug))]
pub struct IggyBatch {
    pub messages: Vec<IggyMessage>,

    payload_type: u8,
    last_offset_delta: u32,
    last_timestamp_delta: u32,
    release: u32,
    attributes: u32,
    base_offset: u64,
    batch_length: u64,
    origin_timestamp: u64,
    base_timestamp: u64,
    reserved_nonce: u128,
    parent: u128,
    checksum_body: u128,
    checksum: u128,
}

impl IggyBatch {
    pub fn new(messages: Vec<Message>) -> Self {
        // Prayge to the compiler gods, this will get optimized away..
        let messages = messages.into_iter().map(Into::into).collect();
        Self {
            payload_type: 0,
            last_offset_delta: 0,
            last_timestamp_delta: 0,
            release: 0,
            attributes: 0,
            base_offset: 0,
            batch_length: 0,
            origin_timestamp: 0,
            base_timestamp: 0,
            reserved_nonce: 0,
            parent: 0,
            checksum_body: 0,
            checksum: 0,
            messages,
        }
    }
}

impl Sizeable for IggyBatch {
    fn get_size_bytes(&self) -> crate::utils::byte_size::IggyByteSize {
        let messages_size = self
            .messages
            .iter()
            .map(IggyMessage::get_size_bytes)
            .sum::<IggyByteSize>();
        IggyByteSize::from(IGGY_BATCH_OVERHEAD) + messages_size
    }
}
