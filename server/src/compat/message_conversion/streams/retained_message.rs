use crate::compat::message_conversion::message_stream::MessageStream;
use crate::compat::message_conversion::snapshots::message_snapshot::MessageSnapshot;

use async_stream::try_stream;
use bytes::{BufMut, BytesMut};
use futures::Stream;
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::models::messages::MessageState;
use std::collections::HashMap;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

const BUF_READER_CAPACITY_BYTES: usize = 512 * 1000;

pub struct RetainedMessageStream {
    pub reader: BufReader<File>,
    read_length: u64,
    read_bytes: u64,
}
impl RetainedMessageStream {
    pub fn new(file: File, read_length: u64) -> RetainedMessageStream {
        RetainedMessageStream {
            reader: BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file),
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
                let offset = self.reader.read_u64_le().await?;
                self.read_bytes += 8;

                let state = self.reader.read_u8().await?;
                self.read_bytes += 1;

                let state = MessageState::from_code(state)?;
                let timestamp = self.reader.read_u64_le().await?;
                self.read_bytes += 8;

                let id = self.reader.read_u128_le().await?;
                self.read_bytes += 16;

                let checksum = self.reader.read_u32_le().await?;
                self.read_bytes += 4;

                let headers_length = self.reader.read_u32_le().await?;
                self.read_bytes += 4;

                let headers = match headers_length {
                    0 => None,
                    _ => {
                        let mut headers_payload = BytesMut::with_capacity(headers_length as usize);
                        headers_payload.put_bytes(0, headers_length as usize);
                        self.reader.read_exact(&mut headers_payload).await?;

                        let headers = HashMap::from_bytes(headers_payload.freeze())?;
                        Some(headers)
                    }
                };
                self.read_bytes += headers_length as u64;

                let payload_len = self.reader.read_u32_le().await?;

                let mut payload = BytesMut::with_capacity(payload_len as usize);
                payload.put_bytes(0, payload_len as usize);
                self.reader.read_exact(&mut payload).await?;
                self.read_bytes += 4 + payload_len as u64;

                let message =
                    MessageSnapshot::new(offset, state, timestamp, id, payload.freeze(), checksum, headers);
                yield message;
            }
        }
    }
}
