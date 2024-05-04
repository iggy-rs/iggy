use crate::compat::binary_schema::BinarySchema;
use crate::compat::schema_sampler::BinarySchemaSampler;
use crate::compat::snapshots::message_snapshot::MessageSnapshot;
use crate::server_error::ServerError;
use crate::streaming::utils::file;
use bytes::{BufMut, BytesMut};
use std::fs;

pub struct MessageSampler {
    pub segment_start_offset: u64,
    pub log_path: String,
    pub index_path: String,
}
impl MessageSampler {
    pub fn new(segment_start_offset: u64, log_path: String, index_path: String) -> MessageSampler {
        MessageSampler {
            segment_start_offset,
            log_path,
            index_path,
        }
    }
}

unsafe impl Send for MessageSampler {}
unsafe impl Sync for MessageSampler {}

impl BinarySchemaSampler for MessageSampler {
    async fn try_sample(&self) -> Result<BinarySchema, ServerError> {
        if fs::metadata(&self.log_path).unwrap().len() == 0 {
            return Ok(BinarySchema::RetainedMessageSchema);
        }

        let index_file = file::open(&self.index_path).await?;
        let buffer = Vec::with_capacity(4);
        let (result, buffer) = index_file.read_exact_at(buffer, 0).await;
        if result.is_err() {
            return Err(ServerError::InvalidMessageOffsetFormatConversion);
        }

        let _ = u32::from_le_bytes(buffer.try_into().unwrap());
        let buffer = Vec::with_capacity(4);
        let (result, buffer) = index_file.read_exact_at(buffer, 4).await;
        if result.is_err() {
            return Err(ServerError::InvalidMessageOffsetFormatConversion);
        }

        let end_position = u32::from_le_bytes(buffer.try_into().unwrap());
        let buffer_size = end_position as usize;
        let mut buffer = Vec::with_capacity(buffer_size);
        buffer.put_bytes(0, buffer_size);
        let log_file = file::open(&self.log_path).await?;
        let buffer = BytesMut::with_capacity(buffer_size);
        let (result, buffer) = log_file.read_exact_at(buffer, 8).await;
        if result.is_err() {
            return Err(ServerError::InvalidMessageOffsetFormatConversion);
        }

        let message = MessageSnapshot::try_from(buffer.freeze())
            .map_err(|_| ServerError::InvalidMessageOffsetFormatConversion)?;
        if message.offset != self.segment_start_offset {
            return Err(ServerError::InvalidMessageOffsetFormatConversion);
        }
        Ok(BinarySchema::RetainedMessageSchema)
    }
}
