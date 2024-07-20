use crate::compat::message_conversion::binary_schema::BinarySchema;
use crate::compat::message_conversion::schema_sampler::BinarySchemaSampler;
use crate::compat::message_conversion::snapshots::retained_batch_snapshot::RetainedMessageBatchSnapshot;
use crate::server_error::ServerError;
use crate::streaming::utils::file;
use bytes::{BufMut, Bytes};
use std::fs;

pub struct RetainedMessageBatchSampler {
    pub segment_start_offset: u64,
    pub log_path: String,
    pub index_path: String,
}

impl RetainedMessageBatchSampler {
    pub fn new(
        segment_start_offset: u64,
        log_path: String,
        index_path: String,
    ) -> RetainedMessageBatchSampler {
        RetainedMessageBatchSampler {
            segment_start_offset,
            log_path,
            index_path,
        }
    }
}

unsafe impl Send for RetainedMessageBatchSampler {}
unsafe impl Sync for RetainedMessageBatchSampler {}

impl BinarySchemaSampler for RetainedMessageBatchSampler {
    async fn try_sample(&self) -> Result<BinarySchema, ServerError> {
        let size = fs::metadata(&self.log_path).unwrap().len() as usize;
        if size == 0 {
            return Ok(BinarySchema::RetainedMessageBatchSchema);
        }

        let mut position = 0;
        let index_file = file::open(&self.index_path).await?;
        let buffer = Vec::with_capacity(4);
        let (result, buffer) = index_file.read_exact_at(buffer, 0).await;
        if result.is_err() {
            return Err(ServerError::CannotReadFromFile);
        }

        let _ = u32::from_le_bytes(buffer.try_into().unwrap());
        position += 4;
        let buffer = Vec::with_capacity(4);
        let (result, buffer) = index_file.read_exact_at(buffer, position).await;
        if result.is_err() {
            return Err(ServerError::CannotReadFromFile);
        }

        let _ = u32::from_le_bytes(buffer.try_into().unwrap());
        position += 4;

        let buffer = Vec::with_capacity(4);
        let (second_index_offset_result, _) = index_file.read_exact_at(buffer, position).await;
        position += 4;

        let second_end_position_buffer = Vec::with_capacity(4);
        let (second_end_position_result, second_end_position_buffer) = index_file
            .read_exact_at(second_end_position_buffer, position)
            .await;
        if result.is_err() {
            return Err(ServerError::CannotReadFromFile);
        }

        position += 4;

        let batch_buffer;
        let log_file = file::open(&self.log_path).await?;
        if second_index_offset_result.is_err() && second_end_position_result.is_err() {
            let buffer = Vec::with_capacity(size - position as usize);
            let (result, buffer) = log_file.read_exact_at(buffer, position).await;
            if result.is_err() {
                return Err(ServerError::CannotReadFromFile);
            }
            batch_buffer = buffer;
        } else {
            let buffer_size =
                u32::from_le_bytes(second_end_position_buffer.try_into().unwrap()) as usize;
            let mut buffer = Vec::with_capacity(buffer_size);
            buffer.put_bytes(0, buffer_size);
            let (result, buffer) = log_file.read_exact_at(buffer, position).await;
            if result.is_err() {
                return Err(ServerError::CannotReadFromFile);
            }
            batch_buffer = buffer;
        }
        let batch = RetainedMessageBatchSnapshot::try_from(Bytes::from(batch_buffer))?;
        if batch.base_offset != self.segment_start_offset {
            return Err(ServerError::InvalidBatchBaseOffsetFormatConversion);
        }
        Ok(BinarySchema::RetainedMessageBatchSchema)
    }
}
