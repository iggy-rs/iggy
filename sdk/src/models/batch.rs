use super::messages::IggyMessage;
use crate::{
    messages::send_messages::Message,
    utils::{byte_size::IggyByteSize, sizeable::Sizeable},
};
use rkyv::util::AlignedVec;
use serde_with::serde_as;

const IGGY_BATCH_OVERHEAD: u64 = 16 + 16 + 16 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + 1;

#[serde_as]
#[derive(Debug, Default)]
pub struct IggyBatch {
    payload_type: u8,
    last_offset_delta: u32,
    last_timestamp_delta: u32,
    release: u32,
    attributes: u32,
    pub base_offset: u64,
    batch_length: u64,
    origin_timestamp: u64,
    pub base_timestamp: u64,
    reserved_nonce: u128,
    parent: u128,
    checksum_body: u128,
    checksum: u128,

    //TODO: Create a wrapper around this, so we can derive serde serialier/deserializer.
    pub messages: AlignedVec,
}

#[derive(
    serde::Serialize, serde::Deserialize, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive,
)]
pub struct IggyBatchTwo {
    payload_type: u8,
    last_offset_delta: u32,
    last_timestamp_delta: u32,
    release: u32,
    attributes: u32,
    pub base_offset: u64,
    batch_length: u64,
    origin_timestamp: u64,
    pub base_timestamp: u64,
    reserved_nonce: u128,
    parent: u128,
    checksum_body: u128,
    checksum: u128,

    //TODO: Create a wrapper around this, so we can derive serde serialier/deserializer.
    pub messages: Vec<IggyMessage>,
}
impl IggyBatchTwo {
    pub fn new(messages: Vec<IggyMessage>) -> Self {
        Self {
            messages,
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
            checksum: 0,
            checksum_body: 0,
        }
    }
}

impl IggyBatch {
    pub fn new(messages: AlignedVec) -> Self {
        Self {
            messages,
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
            checksum: 0,
            checksum_body: 0,
        }
    }

    pub fn header_bytes(&self) -> [u8; 69] {
        let header_bytes = [0u8; 69];
        header_bytes
    }
}

impl Sizeable for IggyBatch {
    fn get_size_bytes(&self) -> crate::utils::byte_size::IggyByteSize {
        IggyByteSize::from(IGGY_BATCH_OVERHEAD + self.messages.len() as u64)
    }
}
