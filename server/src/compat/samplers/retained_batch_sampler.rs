use crate::compat::binary_schema::BinarySchema;
use crate::compat::schema_sampler::BinarySchemaSampler;
use crate::compat::snapshots::retained_batch_snapshot::RetainedMessageBatchSnapshot;
use crate::streaming::utils::file;
use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use iggy::error::IggyError;
use tokio::io::AsyncReadExt;
use tracing::error;

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
    async fn try_sample(&self) -> Result<BinarySchema, IggyError> {
        let mut index_file = file::open(&self.index_path).await?;
        let _ = index_file.read_u32_le().await?;
        let _ = index_file.read_u32_le().await?;
        let second_index_offset = index_file.read_u32_le().await;
        let second_end_position = index_file.read_u32_le().await;

        let mut buffer = Vec::new();
        let mut log_file = file::open(&self.log_path).await?;
        if second_index_offset.is_err() && second_end_position.is_err() {
            let _ = log_file.read_to_end(&mut buffer).await?;
        } else {
            let buffer_size = second_end_position.unwrap() as usize;
            buffer.put_bytes(0, buffer_size);
            let _ = log_file.read_exact(&mut buffer).await?;
        }
        let batch = RetainedMessageBatchSnapshot::try_from(Bytes::from(buffer))?;
        if batch.base_offset != self.segment_start_offset {
            return Err(IggyError::InvalidBatchBaseOffsetFormatConversion);
        }
        Ok(BinarySchema::RetainedMessageBatchSchema)
    }
}
