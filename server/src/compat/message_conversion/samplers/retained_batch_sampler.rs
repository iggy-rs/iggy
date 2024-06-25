use crate::compat::message_conversion::binary_schema::BinarySchema;
use crate::compat::message_conversion::schema_sampler::BinarySchemaSampler;
use crate::compat::message_conversion::snapshots::retained_batch_snapshot::RetainedMessageBatchSnapshot;
use crate::server_error::ServerError;
use crate::streaming::utils::file;
use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use tokio::io::AsyncReadExt;

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

#[async_trait]
impl BinarySchemaSampler for RetainedMessageBatchSampler {
    async fn try_sample(&self) -> Result<BinarySchema, ServerError> {
        let mut index_file = file::open(&self.index_path).await?;
        let mut log_file = file::open(&self.log_path).await?;
        let log_file_size = log_file.metadata().await?.len();

        if log_file_size == 0 {
            return Ok(BinarySchema::RetainedMessageBatchSchema);
        }

        let _ = index_file.read_u32_le().await?;
        let _ = index_file.read_u32_le().await?;
        let second_index_offset = index_file.read_u32_le().await;
        let second_end_position = index_file.read_u32_le().await;

        let mut buffer = Vec::new();
        if second_index_offset.is_err() && second_end_position.is_err() {
            let _ = log_file.read_to_end(&mut buffer).await?;
        } else {
            let buffer_size = second_end_position.unwrap() as usize;
            buffer.put_bytes(0, buffer_size);
            let _ = log_file.read_exact(&mut buffer).await?;
        }
        let batch = RetainedMessageBatchSnapshot::try_from(Bytes::from(buffer))?;
        if batch.base_offset != self.segment_start_offset {
            return Err(ServerError::InvalidBatchBaseOffsetFormatConversion);
        }
        Ok(BinarySchema::RetainedMessageBatchSchema)
    }
}
