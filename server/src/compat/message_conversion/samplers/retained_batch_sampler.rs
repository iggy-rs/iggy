use crate::compat::message_conversion::binary_schema::BinarySchema;
use crate::compat::message_conversion::schema_sampler::BinarySchemaSampler;
use crate::compat::message_conversion::snapshots::retained_batch_snapshot::RetainedMessageBatchSnapshot;
use crate::server_error::CompatError;
use crate::streaming::utils::file;
use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use error_set::ResultContext;
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
    async fn try_sample(&self) -> Result<BinarySchema, CompatError> {
        let mut index_file = file::open(&self.index_path).await.with_error(|_| {
            format!(
                "MESSAGE_CONVERSION_SAMPLER - failed to open index path: {}",
                self.index_path
            )
        })?;
        let mut log_file = file::open(&self.log_path).await.with_error(|_| {
            format!(
                "MESSAGE_CONVERSION_SAMPLER - failed to open log path: {}",
                self.log_path
            )
        })?;
        let log_file_size = log_file
            .metadata()
            .await
            .with_error(|_| {
                format!(
                    "MESSAGE_CONVERSION_SAMPLER - failed to get log file metadata: {}",
                    self.log_path
                )
            })?
            .len();

        if log_file_size == 0 {
            return Ok(BinarySchema::RetainedMessageBatchSchema);
        }

        let _ = index_file.read_u32_le().await.with_error(|_| {
            format!(
                "MESSAGE_CONVERSION_SAMPLER - failed to read first index entry from index path: {}",
                self.index_path
            )
        })?;
        let _ = index_file.read_u32_le().await.with_error(|_| {
            format!("MESSAGE_CONVERSION_SAMPLER - failed to read second index entry from index path: {}", self.index_path)
        })?;
        let second_index_offset = index_file.read_u32_le().await.with_error(|_| {
            format!(
                "MESSAGE_CONVERSION_SAMPLER - failed to read third index entry from index path: {}",
                self.index_path
            )
        });
        let second_end_position = index_file.read_u32_le().await.with_error(|_| {
            format!("MESSAGE_CONVERSION_SAMPLER - failed to read fourth index entry from index path: {}", self.index_path)
        });

        let mut buffer = Vec::new();
        if second_index_offset.is_err() && second_end_position.is_err() {
            let _ = log_file.read_to_end(&mut buffer).await.with_error(|_| {
                format!(
                    "MESSAGE_CONVERSION_SAMPLER - failed to read log file to end: {}",
                    self.log_path
                )
            })?;
        } else {
            let buffer_size = second_end_position.unwrap() as usize;
            buffer.put_bytes(0, buffer_size);
            let _ = log_file.read_exact(&mut buffer).await.with_error(|_| {
                format!(
                    "MESSAGE_CONVERSION_SAMPLER - failed to read exact range from log file: {}",
                    self.log_path
                )
            })?;
        }
        let batch = RetainedMessageBatchSnapshot::try_from(Bytes::from(buffer)).with_error(|_| {
                "MESSAGE_CONVERSION_SAMPLER - failed to create RetainedMessageBatchSnapshot from buffer".to_string()
            })?;
        if batch.base_offset != self.segment_start_offset {
            return Err(CompatError::InvalidBatchBaseOffsetFormatConversion);
        }
        Ok(BinarySchema::RetainedMessageBatchSchema)
    }
}
