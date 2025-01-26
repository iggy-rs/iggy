use rkyv::util::AlignedVec;
use serde_with::serde_as;
use tokio::io::AsyncWriteExt;

use crate::utils::{byte_size::IggyByteSize, sizeable::Sizeable};

pub const IGGY_BATCH_OVERHEAD: u64 = 16 + 16 + 16 + 16 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + 1;

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

    //TODO: This can be Vec<u8>.
    pub messages: AlignedVec,
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

    pub fn write_header<W: std::io::Write>(&self, writer: &mut W) {
        writer.write_all(&self.payload_type.to_le_bytes()).unwrap();
        writer
            .write_all(&self.last_offset_delta.to_le_bytes())
            .unwrap();
        writer
            .write_all(&self.last_timestamp_delta.to_le_bytes())
            .unwrap();
        writer.write_all(&self.release.to_le_bytes()).unwrap();
        writer.write_all(&self.attributes.to_le_bytes()).unwrap();
        writer.write_all(&self.base_offset.to_le_bytes()).unwrap();
        writer.write_all(&self.batch_length.to_le_bytes()).unwrap();
        writer
            .write_all(&self.origin_timestamp.to_le_bytes())
            .unwrap();
        writer
            .write_all(&self.base_timestamp.to_le_bytes())
            .unwrap();
        writer
            .write_all(&self.reserved_nonce.to_le_bytes())
            .unwrap();
        writer.write_all(&self.parent.to_le_bytes()).unwrap();
        writer.write_all(&self.checksum_body.to_le_bytes()).unwrap();
        writer.write_all(&self.checksum.to_le_bytes()).unwrap();
    }

    pub async fn write_messages<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) {
        writer.write_all(&self.messages).await.unwrap();
    }
}

impl Sizeable for IggyBatch {
    fn get_size_bytes(&self) -> crate::utils::byte_size::IggyByteSize {
        IggyByteSize::from(IGGY_BATCH_OVERHEAD + self.messages.len() as u64)
    }
}
