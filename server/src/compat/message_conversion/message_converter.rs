use crate::compat::message_conversion::samplers::message_sampler::MessageSampler;
use crate::compat::message_conversion::samplers::retained_batch_sampler::RetainedMessageBatchSampler;
use crate::compat::message_conversion::schema_sampler::BinarySchemaSampler;
use crate::streaming::sizeable::Sizeable;
use bytes::{BufMut, BytesMut};
use iggy::error::IggyError;

use crate::streaming::segments::storage::{INDEX_SIZE, TIME_INDEX_SIZE};
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub trait Extendable {
    fn extend(&self, bytes: &mut BytesMut);
}

pub trait MessageFormatConverterPersister<W: AsyncWrite> {
    async fn persist(&self, writer: &mut W) -> Result<(), IggyError>;
    async fn persist_index(
        &self,
        position: u32,
        relative_offset: u32,
        writer: &mut W,
    ) -> Result<(), IggyError>;
    async fn persist_time_index(
        &self,
        timestamp: u64,
        relative_offset: u32,
        writer: &mut W,
    ) -> Result<(), IggyError>;
}

impl<W: AsyncWrite + Unpin, T> MessageFormatConverterPersister<W> for T
where
    T: Sizeable + Extendable,
{
    async fn persist(&self, writer: &mut W) -> Result<(), IggyError> {
        let size = self.get_size_bytes();
        let mut batch_bytes = BytesMut::with_capacity(size as usize);
        self.extend(&mut batch_bytes);

        writer.write_all(&batch_bytes).await?;
        Ok(())
    }

    async fn persist_index(
        &self,
        position: u32,
        relative_offset: u32,
        writer: &mut W,
    ) -> Result<(), IggyError> {
        let mut index_bytes = BytesMut::with_capacity(INDEX_SIZE as usize);
        index_bytes.put_u32_le(relative_offset);
        index_bytes.put_u32_le(position);

        writer.write_all(&index_bytes).await?;
        Ok(())
    }

    async fn persist_time_index(
        &self,
        timestamp: u64,
        relative_offset: u32,
        writer: &mut W,
    ) -> Result<(), IggyError> {
        let mut time_index_bytes = BytesMut::with_capacity(TIME_INDEX_SIZE as usize);
        time_index_bytes.put_u32_le(relative_offset);
        time_index_bytes.put_u64_le(timestamp);

        writer.write_all(&time_index_bytes).await?;
        Ok(())
    }
}

pub struct MessageFormatConverter {
    pub samplers: Vec<Box<dyn BinarySchemaSampler>>,
}

impl MessageFormatConverter {
    pub fn init(
        segment_start_offset: u64,
        log_path: String,
        index_path: String,
    ) -> MessageFormatConverter {
        // Always append new schemas to beginning of vec
        MessageFormatConverter {
            samplers: vec![
                Box::new(RetainedMessageBatchSampler::new(
                    segment_start_offset,
                    log_path.clone(),
                    index_path.clone(),
                )),
                Box::new(MessageSampler::new(
                    segment_start_offset,
                    log_path,
                    index_path,
                )),
            ],
        }
    }
}
