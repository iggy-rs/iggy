use crate::compat::binary_schema::BinarySchema;
use crate::compat::schema_sampler::BinarySchemaSampler;
use crate::compat::snapshots::message_snapshot::MessageSnapshot;
use crate::compat::snapshots::retained_batch_snapshot::RetainedMessageBatchSnapshot;
use crate::streaming::utils::file;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use futures::Stream;
use iggy::error::IggyError;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};

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
        let _ = index_file.read_u32_le().await?;
        let end_position = index_file.read_u32_le().await?;

        let mut log_file = file::open(&self.log_path).await?;
        let buffer_size = end_position as usize;
        let mut buffer = BytesMut::with_capacity(buffer_size);
        buffer.put_bytes(0, buffer_size);
        let _ = log_file.read_exact(&mut buffer).await?;

        let batch = RetainedMessageBatchSnapshot::try_from(buffer.freeze())?;
        if batch.base_offset != self.segment_start_offset {
            return Err(IggyError::InvalidBatchBaseOffsetFormatConversion);
        }
        Ok(BinarySchema::RetainedMessageBatchSchema)
    }
}

pub struct MessageStream {
    pub reader: BufReader<File>,
    pub read_bytes: u64,
}

impl Stream for MessageStream {
    type Item = Result<MessageSnapshot, IggyError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let offset = self.reader.read_u64_le();
        // if offset.is_err() {
        // }
        self.read_bytes += 8;

        let state = self.reader.read_u8().await;
        if state.is_err() {
            return Err(IggyError::CannotReadMessageState);
        }
        self.read_bytes += 1;

        let state = MessageState::from_code(state.unwrap())?;
        let timestamp = reader.read_u64_le().await;
        if timestamp.is_err() {
            return Err(IggyError::CannotReadMessageTimestamp);
        }
        read_bytes += 8;

        let id = reader.read_u128_le().await;
        if id.is_err() {
            return Err(IggyError::CannotReadMessageId);
        }
        read_bytes += 16;

        let checksum = reader.read_u32_le().await;
        if checksum.is_err() {
            return Err(IggyError::CannotReadMessageChecksum);
        }
        read_bytes += 4;

        let headers_length = reader.read_u32_le().await;
        if headers_length.is_err() {
            return Err(IggyError::CannotReadHeadersLength);
        }
        read_bytes += 4;

        let headers_length = headers_length.unwrap();
        let headers = match headers_length {
            0 => None,
            _ => {
                let mut headers_payload = BytesMut::with_capacity(headers_length as usize);
                headers_payload.put_bytes(0, headers_length as usize);
                if reader.read_exact(&mut headers_payload).await.is_err() {
                    return Err(IggyError::CannotReadHeadersPayload);
                }

                let headers = HashMap::from_bytes(headers_payload.freeze())?;
                Some(headers)
            }
        };
        read_bytes += headers_length as u64;

        let payload_len = reader.read_u32_le().await;
        if payload_len.is_err() {
            return Err(IggyError::CannotReadMessageLength);
        }

        let payload_len = payload_len.unwrap();
        let mut payload = BytesMut::with_capacity(payload_len as usize);
        payload.put_bytes(0, payload_len as usize);
        if reader.read_exact(&mut payload).await.is_err() {
            return Err(IggyError::CannotReadMessagePayload);
        }
        read_bytes += 4 + payload_len as u64;

        let payload = payload.freeze();
        let offset = offset.unwrap();
        let timestamp = timestamp.unwrap();
        let id = id.unwrap();
        let checksum = checksum.unwrap();

        let message =
            MessageSnapshot::new(offset, state, timestamp, id, payload, checksum, headers);
    }
}
