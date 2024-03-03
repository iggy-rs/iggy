use crate::compat::schema_sampler::BinarySchemaSampler;
use crate::compat::schemas::message::Message;
use crate::streaming::utils::file;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use iggy::error::IggyError;
use tokio::io::AsyncReadExt;
use crate::compat::binary_schema::BinarySchema;

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

#[async_trait]
impl BinarySchemaSampler for MessageSampler {
    async fn try_sample(&self) -> Result<BinarySchema, IggyError> {
        let mut index_file = file::open(&self.index_path).await?;
        let _ = index_file.read_u32_le().await?;
        let end_position = index_file.read_u32_le().await?;

        let mut log_file = file::open(&self.log_path).await?;
        let buffer_size = end_position as usize;
        let mut buffer = BytesMut::with_capacity(buffer_size);
        buffer.put_bytes(0, buffer_size);
        let _ = log_file.read_exact(&mut buffer).await?;

        let message = Message::try_from(buffer.freeze())?;
        if message.offset != self.segment_start_offset {
            return Err(IggyError::InvalidMessageOffsetFormatConversion);
        }
        Ok(BinarySchema::RetainedMessageSchema)
    }
}
