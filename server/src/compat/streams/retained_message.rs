use crate::compat::message_stream::MessageStream;
use crate::compat::snapshots::message_snapshot::MessageSnapshot;

use async_stream::try_stream;
use bytes::{BufMut, BytesMut};
use futures::Stream;
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::models::messages::MessageState;
use monoio::fs::File;
use std::collections::HashMap;

// TODO: monoio: used raw file instead of BufReader
pub struct RetainedMessageStream {
    pub file: File,
    read_length: u64,
    read_bytes: u64,
}

impl RetainedMessageStream {
    pub fn new(file: File, read_length: u64) -> RetainedMessageStream {
        RetainedMessageStream {
            file,
            read_bytes: 0,
            read_length,
        }
    }
}

impl MessageStream for RetainedMessageStream {
    type Item = Result<MessageSnapshot, IggyError>;

    fn into_stream(mut self) -> impl Stream<Item = Self::Item> {
        try_stream! {
            while self.read_bytes < self.read_length {
                // let mut buffer = [0u8, 8];
                let buffer = Vec::with_capacity(8);
                let (result, buffer) = self.file.read_exact_at(buffer, 0).await;
                let offset = u64::from_le_bytes(buffer.try_into().unwrap());
                self.read_bytes += 8;

                let buffer = Vec::with_capacity(1);
                let (result, buffer) = self.file.read_exact_at(buffer, self.read_bytes).await;
                let state  = u8::from_le_bytes(buffer.try_into().unwrap());
                self.read_bytes += 1;

                let state = MessageState::from_code(state)?;
                let timestamp = self.file.read_u64_le().await?;
                self.read_bytes += 8;

                let id = self.file.read_u128_le().await?;
                self.read_bytes += 16;

                let checksum = self.file.read_u32_le().await?;
                self.read_bytes += 4;

                let headers_length = self.file.read_u32_le().await?;
                self.read_bytes += 4;

                let headers = match headers_length {
                    0 => None,
                    _ => {
                        let mut headers_payload = BytesMut::with_capacity(headers_length as usize);
                        headers_payload.put_bytes(0, headers_length as usize);
                        self.file.read_exact(&mut headers_payload).await?;

                        let headers = HashMap::from_bytes(headers_payload.freeze())?;
                        Some(headers)
                    }
                };
                self.read_bytes += headers_length as u64;

                let payload_len = self.file.read_u32_le().await?;

                let mut payload = BytesMut::with_capacity(payload_len as usize);
                payload.put_bytes(0, payload_len as usize);
                self.file.read_exact(&mut payload).await?;
                self.read_bytes += 4 + payload_len as u64;

                let message =
                    MessageSnapshot::new(offset, state, timestamp, id, payload.freeze(), checksum, headers);
                yield message;
            }
        }
    }
}
