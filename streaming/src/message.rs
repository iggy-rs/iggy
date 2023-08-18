use crate::utils::{checksum, timestamp};
use bytes::Bytes;
use iggy::bytes_serializable::BytesSerializable;
use iggy::header;
use iggy::header::{HeaderKey, HeaderValue};
use iggy::messages::send_messages;
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub offset: u64,
    pub timestamp: u64,
    pub id: u128,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
    #[serde(skip)]
    pub checksum: u32,
    #[serde(skip)]
    pub length: u32,
    #[serde_as(as = "Base64")]
    pub payload: Bytes,
}

impl Message {
    pub fn from_message(message: &send_messages::Message) -> Self {
        let timestamp = timestamp::get();
        let checksum = checksum::calculate(&message.payload);
        let headers = message.headers.as_ref().map(|headers| headers.clone());

        Self::empty(
            timestamp,
            message.id,
            message.payload.clone(),
            checksum,
            headers,
        )
    }

    pub fn empty(
        timestamp: u64,
        id: u128,
        payload: Bytes,
        checksum: u32,
        headers: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> Self {
        Message::create(0, timestamp, id, payload, checksum, headers)
    }

    pub fn create(
        offset: u64,
        timestamp: u64,
        id: u128,
        payload: Bytes,
        checksum: u32,
        headers: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> Self {
        Message {
            offset,
            timestamp,
            id,
            checksum,
            length: payload.len() as u32,
            payload,
            headers,
        }
    }

    pub fn get_size_bytes(&self, with_checksum: bool) -> u32 {
        // Offset + Timestamp + ID + Length + Payload + Headers
        let size = 8 + 8 + 16 + 4 + self.length + header::get_headers_size_bytes(&self.headers);
        if with_checksum {
            size + 4
        } else {
            size
        }
    }

    pub fn extend(&self, bytes: &mut Vec<u8>, with_checksum: bool) {
        bytes.extend(self.offset.to_le_bytes());
        bytes.extend(self.timestamp.to_le_bytes());
        bytes.extend(self.id.to_le_bytes());
        if with_checksum {
            bytes.extend(self.checksum.to_le_bytes());
        }
        if let Some(headers) = &self.headers {
            let headers_bytes = headers.as_bytes();
            bytes.extend((headers_bytes.len() as u32).to_le_bytes());
            bytes.extend(&headers_bytes);
        } else {
            bytes.extend(0u32.to_le_bytes());
        }
        bytes.extend(self.length.to_le_bytes());
        bytes.extend(&self.payload);
    }
}
