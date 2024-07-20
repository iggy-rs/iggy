use crate::compat::message_conversion::message_stream::MessageStream;
use crate::compat::message_conversion::snapshots::message_snapshot::MessageSnapshot;

use async_stream::try_stream;
use bytes::{BufMut, BytesMut};
use futures::Stream;
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::models::messages::MessageState;
use monoio::fs::File;
use std::collections::HashMap;
use tracing::error;

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
                let buffer = Vec::with_capacity(8);
                let (result, buffer) = self.file.read_exact_at(buffer, 0).await;
                if result.is_err() {
                    error!("Failed to read offset from retained message file");
                    break;
                }

                let offset = u64::from_le_bytes(buffer.try_into().unwrap());
                self.read_bytes += 8;

                let buffer = Vec::with_capacity(1);
                let (result, buffer) = self.file.read_exact_at(buffer, self.read_bytes).await;
                if result.is_err() {
                    error!("Failed to read state from retained message file");
                    break;
                }

                let state  = u8::from_le_bytes(buffer.try_into().unwrap());
                self.read_bytes += 1;

                let state = MessageState::from_code(state)?;
                let buffer = Vec::with_capacity(8);
                let (result, buffer) = self.file.read_exact_at(buffer, self.read_bytes).await;
                if result.is_err() {
                    error!("Failed to read timestamp from retained message file");
                    break;
                }

                let timestamp = u64::from_le_bytes(buffer.try_into().unwrap());
                self.read_bytes += 8;

                let buffer = Vec::with_capacity(16);
                let (result, buffer) = self.file.read_exact_at(buffer, self.read_bytes).await;
                if result.is_err() {
                    error!("Failed to read ID from retained message file");
                    break;
                }

                let id = u128::from_le_bytes(buffer.try_into().unwrap());
                self.read_bytes += 16;

                let buffer = Vec::with_capacity(4);
                let (result, buffer) = self.file.read_exact_at(buffer, self.read_bytes).await;
                if result.is_err() {
                    error!("Failed to read checksum from retained message file");
                    break;
                }

                let checksum = u32::from_le_bytes(buffer.try_into().unwrap());
                self.read_bytes += 4;

                let buffer = Vec::with_capacity(4);
                let (result, buffer) = self.file.read_exact_at(buffer, self.read_bytes).await;
                if result.is_err() {
                    error!("Failed to read headers length from retained message file");
                    break;
                }

                let headers_length = u32::from_le_bytes(buffer.try_into().unwrap());
                self.read_bytes += 4;

                let headers = match headers_length {
                    0 => None,
                    _ => {
                        let mut headers_payload = BytesMut::with_capacity(headers_length as usize);
                        headers_payload.put_bytes(0, headers_length as usize);
                        let (result, headers_payload) = self.file.read_exact_at(headers_payload, self.read_bytes).await;
                        if result.is_err() {
                            error!("Failed to read headers from retained message file");
                            break;
                        }

                        self.read_bytes += headers_length as u64;
                        let headers = HashMap::from_bytes(headers_payload.freeze())?;
                        Some(headers)
                    }
                };

                let buffer = Vec::with_capacity(4);
                let (result, buffer) = self.file.read_exact_at(buffer, self.read_bytes).await;
                if result.is_err() {
                    error!("Failed to read payload length from retained message file");
                    break;
                }

                let payload_len = u32::from_le_bytes(buffer.try_into().unwrap());
                self.read_bytes += 4;

                let mut payload = BytesMut::with_capacity(payload_len as usize);
                payload.put_bytes(0, payload_len as usize);
                let (result, payload) = self.file.read_exact_at(payload, self.read_bytes).await;
                if result.is_err() {
                    error!("Failed to read payload from retained message file");
                    break;
                }

                self.read_bytes += 4 + payload_len as u64;
                let message =
                    MessageSnapshot::new(offset, state, timestamp, id, payload.freeze(), checksum, headers);
                yield message;
            }
        }
    }
}
