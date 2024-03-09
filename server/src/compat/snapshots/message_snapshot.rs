use crate::compat::message_converter::Extendable;
use crate::streaming::sizeable::Sizeable;
use bytes::{BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::models::header::{self, HeaderKey, HeaderValue};
use iggy::models::messages::MessageState;
use std::collections::HashMap;
use std::convert::TryFrom;

#[derive(Debug)]
pub struct MessageSnapshot {
    pub offset: u64,
    pub state: MessageState,
    pub timestamp: u64,
    pub id: u128,
    pub payload: Bytes,
    pub checksum: u32,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
}

impl MessageSnapshot {
    pub fn new(
        offset: u64,
        state: MessageState,
        timestamp: u64,
        id: u128,
        payload: Bytes,
        checksum: u32,
        headers: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> MessageSnapshot {
        MessageSnapshot {
            offset,
            state,
            timestamp,
            id,
            payload,
            checksum,
            headers,
        }
    }
}

impl Extendable for MessageSnapshot {
    fn extend(&self, bytes: &mut BytesMut) {
        let length = self.get_size_bytes() - 4;
        let id = self.id;
        let offset = self.offset;
        let timestamp = self.timestamp;
        let payload = self.payload.clone();
        let checksum = self.checksum;
        let message_state = self.state;
        let headers = &self.headers;

        bytes.put_u32_le(length);
        bytes.put_u64_le(offset);
        bytes.put_u8(message_state.as_code());
        bytes.put_u64_le(timestamp);
        bytes.put_u128_le(id);
        bytes.put_u32_le(checksum);
        if let Some(headers) = headers {
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u32_le(headers.len() as u32);
            bytes.put_slice(&headers.as_bytes());
        } else {
            bytes.put_u32_le(0u32);
        }
        bytes.put_slice(&payload);
    }
}

impl Sizeable for MessageSnapshot {
    fn get_size_bytes(&self) -> u32 {
        let headers_size = header::get_headers_size_bytes(&self.headers);
        41 + headers_size + self.payload.len() as u32
    }
}

impl TryFrom<Bytes> for MessageSnapshot {
    type Error = IggyError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let offset = u64::from_le_bytes(
            value
                .get(0..8)
                .ok_or_else(|| {
                    IggyError::CannotReadMessageFormatConversion(
                        "Failed to read message offset".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let state = MessageState::from_code(*value.get(8).ok_or_else(|| {
            IggyError::CannotReadMessageFormatConversion("Failed to read message state".to_owned())
        })?)?;
        let timestamp = u64::from_le_bytes(
            value
                .get(9..17)
                .ok_or_else(|| {
                    IggyError::CannotReadMessageFormatConversion(
                        "Failed to read message timestamp".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let id = u128::from_le_bytes(
            value
                .get(17..33)
                .ok_or_else(|| {
                    IggyError::CannotReadMessageFormatConversion(
                        "Failed to read message id".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let checksum = u32::from_le_bytes(
            value
                .get(33..37)
                .ok_or_else(|| {
                    IggyError::CannotReadMessageFormatConversion(
                        "Failed to read message checksum".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let headers_length = u32::from_le_bytes(
            value
                .get(37..41)
                .ok_or_else(|| {
                    IggyError::CannotReadMessageFormatConversion(
                        "Failed to read headers length".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let headers = match headers_length {
            0 => None,
            _ => {
                let headers_payload = &value[41..(41 + headers_length as usize)];
                let headers = HashMap::from_bytes(Bytes::copy_from_slice(headers_payload))
                    .map_err(|_| {
                        IggyError::CannotReadMessageFormatConversion(
                            "Failed to read message headers".to_owned(),
                        )
                    })?;
                Some(headers)
            }
        };

        let position = 41 + headers_length as usize;
        let payload_length = u32::from_le_bytes(
            value
                .get(position..(position + 4))
                .ok_or_else(|| {
                    IggyError::CannotReadMessageFormatConversion(
                        "Failed to read message payload length".to_owned(),
                    )
                })?
                .try_into()?,
        );
        let payload =
            Bytes::copy_from_slice(&value[position + 4..position + 4 + payload_length as usize]);

        Ok(MessageSnapshot {
            offset,
            state,
            timestamp,
            id,
            payload,
            checksum,
            headers,
        })
    }
}
