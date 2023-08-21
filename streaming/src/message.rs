use crate::utils::{checksum, timestamp};
use bytes::{BufMut, Bytes};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::Error;
use iggy::header;
use iggy::header::{HeaderKey, HeaderValue};
use iggy::messages::send_messages;
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub offset: u64,
    pub state: MessageState,
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

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageState {
    Available,
    Unavailable,
    Poisoned,
    MarkedForDeletion,
}

impl MessageState {
    pub fn as_code(&self) -> u8 {
        match self {
            MessageState::Available => 1,
            MessageState::Unavailable => 10,
            MessageState::Poisoned => 20,
            MessageState::MarkedForDeletion => 30,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            1 => Ok(MessageState::Available),
            10 => Ok(MessageState::Unavailable),
            20 => Ok(MessageState::Poisoned),
            30 => Ok(MessageState::MarkedForDeletion),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Display for MessageState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageState::Available => write!(f, "available"),
            MessageState::Unavailable => write!(f, "unavailable"),
            MessageState::Poisoned => write!(f, "poisoned"),
            MessageState::MarkedForDeletion => write!(f, "marked_for_deletion"),
        }
    }
}

impl FromStr for MessageState {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "available" => Ok(MessageState::Available),
            "unavailable" => Ok(MessageState::Unavailable),
            "poisoned" => Ok(MessageState::Poisoned),
            "marked_for_deletion" => Ok(MessageState::MarkedForDeletion),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Message {
    pub fn from_message(message: &send_messages::Message) -> Self {
        let timestamp = timestamp::get();
        let checksum = checksum::calculate(&message.payload);
        let headers = message.headers.as_ref().map(|headers| headers.clone());

        Self::empty(
            timestamp,
            MessageState::Available,
            message.id,
            message.payload.clone(),
            checksum,
            headers,
        )
    }

    pub fn empty(
        timestamp: u64,
        state: MessageState,
        id: u128,
        payload: Bytes,
        checksum: u32,
        headers: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> Self {
        Message::create(0, state, timestamp, id, payload, checksum, headers)
    }

    pub fn create(
        offset: u64,
        state: MessageState,
        timestamp: u64,
        id: u128,
        payload: Bytes,
        checksum: u32,
        headers: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> Self {
        Message {
            offset,
            state,
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

    pub fn extend(&self, bytes: &mut Vec<u8>, with_state: bool, with_checksum: bool) {
        bytes.extend(self.offset.to_le_bytes());
        if with_state {
            bytes.put_u8(self.state.as_code());
        }
        bytes.put_u64_le(self.timestamp);
        bytes.put_u128_le(self.id);
        if with_checksum {
            bytes.put_u32_le(self.checksum);
        }
        if let Some(headers) = &self.headers {
            let headers_bytes = headers.as_bytes();
            bytes.put_u32_le(headers_bytes.len() as u32);
            bytes.extend(&headers_bytes);
        } else {
            bytes.put_u32_le(0u32);
        }
        bytes.put_u32_le(self.length);
        bytes.extend(&self.payload);
    }
}
