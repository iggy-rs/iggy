use std::{io::Write, sync::Arc};

use rkyv::util::AlignedVec;
use serde_with::serde_as;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
};
use tracing::warn;

use crate::utils::{byte_size::IggyByteSize, sizeable::Sizeable};

pub const IGGY_BATCH_OVERHEAD: u64 = 16 + 16 + 16 + 16 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + 1;

#[derive(Debug, Default, Copy, Clone)]
pub struct IggyHeader {
    payload_type: u8,
    pub last_offset_delta: u32,
    last_timestamp_delta: u32,
    release: u32,
    attributes: u32,
    pub base_offset: u64,
    pub batch_length: u64,
    origin_timestamp: u64,
    pub base_timestamp: u64,
    reserved_nonce: u128,
    parent: u128,
    checksum_body: u128,
    checksum: u128,
}

impl IggyHeader {
    pub fn new(
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
    ) -> Self {
        Self {
            payload_type,
            last_offset_delta,
            last_timestamp_delta,
            release,
            attributes,
            base_offset,
            batch_length,
            origin_timestamp,
            base_timestamp,
            reserved_nonce,
            parent,
            checksum_body,
            checksum,
        }
    }

    pub fn write_header<const N: usize>(&self, writer: &mut HeaderWriter<N>) {
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

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let payload_type = bytes[offset];
        offset += 1;

        let last_offset_delta = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let last_timestamp_delta =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let release = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let attributes = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let base_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let batch_length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let origin_timestamp = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let base_timestamp = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let reserved_nonce = u128::from_le_bytes(bytes[offset..offset + 16].try_into().unwrap());
        offset += 16;

        let parent = u128::from_le_bytes(bytes[offset..offset + 16].try_into().unwrap());
        offset += 16;

        let checksum_body = u128::from_le_bytes(bytes[offset..offset + 16].try_into().unwrap());
        offset += 16;

        let checksum = u128::from_le_bytes(bytes[offset..offset + 16].try_into().unwrap());

        Self {
            payload_type,
            last_offset_delta,
            last_timestamp_delta,
            release,
            attributes,
            base_offset,
            batch_length,
            origin_timestamp,
            base_timestamp,
            reserved_nonce,
            parent,
            checksum_body,
            checksum,
        }
    }

    pub fn as_bytes(&self) -> [u8; IGGY_BATCH_OVERHEAD as usize] {
        let header = [0u8; IGGY_BATCH_OVERHEAD as usize];
        let mut writer = HeaderWriter::new(header);
        self.write_header(&mut writer);
        writer.header()
    }
}

//TODO create a trait for this that will expose two methods async and not async.
pub struct HeaderWriter<const N: usize> {
    buffer: [u8; N],
    position: usize,
}

impl<const N: usize> HeaderWriter<N> {
    pub fn new(buffer: [u8; N]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }

    pub fn header(self) -> [u8; N] {
        self.buffer
    }
}

impl<const N: usize> From<[u8; N]> for HeaderWriter<N> {
    fn from(buffer: [u8; N]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }
}

impl<const N: usize> std::io::Write for HeaderWriter<N> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let available = N.saturating_sub(self.position);
        let len = buf.len().min(available);
        self.buffer[self.position..self.position + len].copy_from_slice(&buf[..len]);
        self.position += len;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[serde_as]
#[derive(Clone, Debug, Default)]
pub struct IggyBatch {
    pub header: IggyHeader,
    //TODO: This can be Arc<Vec<u8>>.
    pub messages: Arc<AlignedVec>,
}

impl IggyBatch {
    pub fn new(messages: AlignedVec) -> Self {
        let messages = Arc::new(messages);
        let header = Default::default();
        Self { header, messages }
    }

    pub fn write_header<const N: usize>(&self, writer: &mut HeaderWriter<N>) {
        self.header.write_header(writer);
    }

    pub async fn read_header(reader: &mut BufReader<File>) -> IggyHeader {
        let payload_type = reader.read_u8().await.unwrap();
        let last_offset_delta = reader.read_u32_le().await.unwrap();
        let last_timestamp_delta = reader.read_u32_le().await.unwrap();
        let release = reader.read_u32_le().await.unwrap();
        let attributes = reader.read_u32_le().await.unwrap();
        let base_offset = reader.read_u64_le().await.unwrap();
        let batch_length = reader.read_u64_le().await.unwrap();
        let origin_timestamp = reader.read_u64_le().await.unwrap();
        let base_timestamp = reader.read_u64_le().await.unwrap();
        let reserved_nonce = reader.read_u128_le().await.unwrap();
        let parent = reader.read_u128_le().await.unwrap();
        let checksum_body = reader.read_u128_le().await.unwrap();
        let checksum = reader.read_u128_le().await.unwrap();

        IggyHeader::new(
            payload_type,
            last_offset_delta,
            last_timestamp_delta,
            release,
            attributes,
            base_offset,
            batch_length,
            origin_timestamp,
            base_timestamp,
            reserved_nonce,
            parent,
            checksum_body,
            checksum,
        )
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
