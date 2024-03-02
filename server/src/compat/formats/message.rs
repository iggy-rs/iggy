use bytes::{Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy::models::messages::MessageState;
use std::collections::HashMap;
use std::convert::TryFrom;

#[derive(Debug)]
pub struct Message {
    offset: u64,
    state: MessageState,
    timestamp: u64,
    id: u128,
    payload: Bytes,
    checksum: u32,
    headers: Option<HashMap<HeaderKey, HeaderValue>>,
}

impl TryFrom<Bytes> for Message {
    type Error = IggyError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let offset = u64::from_le_bytes(
            value
                .get(0..8)
                .ok_or_else(|| IggyError::CannotReadMessageFormatConversion)?
                .try_into()?,
        );
        let state = MessageState::from_code(
            *value
                .get(8)
                .ok_or_else(|| IggyError::CannotReadMessageFormatConversion)?,
        )?;
        let timestamp = u64::from_le_bytes(
            value
                .get(9..17)
                .ok_or_else(|| IggyError::CannotReadMessageFormatConversion)?
                .try_into()?,
        );
        let id = u128::from_le_bytes(
            value
                .get(17..33)
                .ok_or_else(|| IggyError::CannotReadMessageFormatConversion)?
                .try_into()?,
        );
        let checksum = u32::from_le_bytes(
            value
                .get(33..37)
                .ok_or_else(|| IggyError::CannotReadMessageFormatConversion)?
                .try_into()?,
        );
        let headers_length = u32::from_le_bytes(
            value
                .get(37..41)
                .ok_or_else(|| IggyError::CannotReadMessageFormatConversion)?
                .try_into()?,
        );
        let headers = match headers_length {
            0 => None,
            _ => {
                let headers_payload = &value[41..(41 + headers_length as usize)];
                let headers = HashMap::from_bytes(Bytes::copy_from_slice(headers_payload))?;
                Some(headers)
            }
        };

        let position = 41 + headers_length as usize;
        let payload_length = u32::from_le_bytes(
            value
                .get(position..(position + 4))
                .ok_or_else(|| IggyError::CannotReadMessageFormatConversion)?
                .try_into()?,
        );
        let payload =
            Bytes::copy_from_slice(&value[position + 4..position + 4 + payload_length as usize]);

        Ok(Message {
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
