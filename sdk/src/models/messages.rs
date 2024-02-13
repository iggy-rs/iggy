use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::messages::send_messages;
use crate::models::header;
use crate::models::header::{HeaderKey, HeaderValue};
use crate::sizeable::Sizeable;
use crate::utils::{checksum, timestamp::IggyTimestamp};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;

pub const POLLED_MESSAGE_METADATA: u32 = 8 + 1 + 8 + 4;
pub const RETAINED_MESSAGE_HEADER: u32 = 8 + 4 + 8 + 4;

/// The wrapper on top of the collection of messages that are polled from the partition.
/// It consists of the following fields:
/// - `partition_id`: the identifier of the partition.
/// - `current_offset`: the current offset of the partition.
/// - `messages`: the collection of messages.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledMessages {
    /// The identifier of the partition.
    pub partition_id: u32,
    /// The current offset of the partition.
    pub current_offset: u64,
    /// The collection of messages.
    pub messages: Vec<PolledMessage>,
}

/// The single message that is polled from the partition.
/// It consists of the following fields:
/// - `offset`: the offset of the message.
/// - `state`: the state of the message.
/// - `timestamp`: the timestamp of the message.
/// - `id`: the identifier of the message.
/// - `checksum`: the checksum of the message, can be used to verify the integrity of the message.
/// - `headers`: the optional headers of the message.
/// - `length`: the length of the payload.
/// - `payload`: the binary payload of the message.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledMessage {
    /// The offset of the message.
    pub offset: u64,
    /// The state of the message.
    pub state: MessageState,
    /// The timestamp of the message.
    pub timestamp: u64,
    /// The identifier of the message.
    pub id: u128,
    /// The checksum of the message, can be used to verify the integrity of the message.
    pub checksum: u32,
    /// The optional headers of the message.
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
    /// The length of the payload.
    #[serde(skip)]
    pub length: u32,
    /// The binary payload of the message.
    #[serde_as(as = "Base64")]
    pub payload: Bytes,
}
/// Represents single message that is used by the system.
/// It consists of the following fields:
/// - `length`: the length of the message.
/// - `bytes`: the binary representation of the message.

#[derive(Debug, Clone)]
pub struct RetainedMessage {
    pub base_offset: u64,
    pub last_offset_delta: u32,
    pub max_timestamp: u64,
    pub length: u32,
    pub bytes: Bytes,
}

impl From<PolledMessage> for RetainedMessage {
    fn from(value: PolledMessage) -> Self {
        let mut payload = BytesMut::with_capacity(
            (POLLED_MESSAGE_METADATA
                + value.length
                + header::get_headers_size_bytes(&value.headers)) as usize,
        );

        payload.put_u64_le(value.offset);
        payload.put_u8(value.state.as_code());
        payload.put_u64_le(value.timestamp);
        payload.put_u128_le(value.id);
        payload.put_u32_le(value.checksum);
        if let Some(headers) = value.headers {
            let headers_bytes = headers.as_bytes();
            #[allow(clippy::cast_possible_truncation)]
            payload.put_u32_le(headers_bytes.len() as u32);
            payload.put_slice(&headers_bytes);
        } else {
            payload.put_u32_le(0u32);
        }
        payload.put_u32_le(value.length);
        payload.put_slice(&value.payload);

        Self {
            base_offset: 1,
            last_offset_delta: 10,
            max_timestamp: 20,
            length: payload.len() as u32,
            bytes: payload.freeze(),
        }
    }
}

impl RetainedMessage {
    pub fn new(length: u32, bytes: Bytes) -> Self {
        Self { base_offset:1, last_offset_delta: 1, max_timestamp: 20, length, bytes }
    }
    /// Creates a new RetainedMessage from the binary representation.
    pub fn from_bytes(
        id_bytes: &[u8],
        offset_bytes: &[u8],
        timestamp_bytes: &[u8],
        message_state_byte: &u8,
        checksum_bytes: &[u8],
        headers_bytes: &[u8],
        payload_bytes: &[u8],
    ) -> Self {
        let mut payload = Vec::with_capacity(
            5 + id_bytes.len()
                + offset_bytes.len()
                + timestamp_bytes.len()
                + checksum_bytes.len()
                + headers_bytes.len()
                + payload_bytes.len(),
        );
        payload.put_slice(offset_bytes);
        payload.put_u8(*message_state_byte);
        payload.put_slice(timestamp_bytes);
        payload.put_slice(id_bytes);
        payload.put_slice(checksum_bytes);
        payload.put_u32_le(headers_bytes.len() as u32);
        payload.put_slice(headers_bytes);
        payload.put_u32_le(payload_bytes.len() as u32);
        payload.put_slice(payload_bytes);

        Self {
            base_offset: 1,
            last_offset_delta: 10,
            max_timestamp: 20,
            length: payload.len() as u32,
            bytes: Bytes::from(payload),
        }
    }
    /// Creates a new RetainedMessage from message and offset.
    pub fn from_message(offset: u64, timestamp: u64, message: &send_messages::Message, payload: &mut BytesMut) {
        let checksum = checksum::calculate(&message.payload);
        let message_state = MessageState::Available;
        let length = message.payload.len() as u32;

        payload.put_u64_le(offset);
        payload.put_u8(message_state.as_code());
        payload.put_u64_le(timestamp);
        payload.put_u128_le(message.id);
        payload.put_u32_le(checksum);
        if let Some(headers) = &message.headers {
            let headers_bytes = headers.as_bytes();
            #[allow(clippy::cast_possible_truncation)]
            payload.put_u32_le(headers_bytes.len() as u32);
            payload.put_slice(&headers_bytes);
        } else {
            payload.put_u32_le(0u32);
        }
        payload.put_u32_le(length);
        payload.put_slice(&message.payload);
    }

    pub fn get_id(&self) -> u128 {
        u128::from_le_bytes(self.bytes[17..33].try_into().unwrap())
    }
    pub fn get_message_state(&self) -> MessageState {
        MessageState::from_code(self.bytes[8]).unwrap()
    }
    pub fn get_offset(&self) -> u64 {
        u64::from_le_bytes(self.bytes[..8].try_into().unwrap())
    }
    pub fn get_timestamp(&self) -> u64 {
        u64::from_le_bytes(self.bytes[9..17].try_into().unwrap())
    }
    pub fn get_checksum(&self) -> u32 {
        u32::from_le_bytes(self.bytes[33..37].try_into().unwrap())
    }
    pub fn try_get_headers(&self) -> Result<Option<HashMap<HeaderKey, HeaderValue>>, IggyError> {
        let headers_length = u32::from_le_bytes(self.bytes[37..41].try_into().unwrap());
        if headers_length == 0 {
            return Ok(None);
        }
        let headers_payload = self.bytes[41..41 + headers_length as usize].to_owned();
        let headers_bytes = Bytes::from(headers_payload);

        Ok(Some(HashMap::from_bytes(headers_bytes)?))
    }
    pub fn get_payload(&self) -> &[u8] {
        let headers_length = u32::from_le_bytes(self.bytes[37..41].try_into().unwrap());
        let payload_length = u32::from_le_bytes(
            self.bytes[(41 + headers_length) as usize..(41 + headers_length + 4) as usize]
                .try_into()
                .unwrap(),
        );
        &self.bytes[(41 + headers_length + 4) as usize
            ..(41 + headers_length + 4 + payload_length) as usize]
    }
    pub fn get_id_bytes(&self) -> &[u8] {
        &self.bytes[17..33]
    }
    pub fn get_offset_bytes(&self) -> &[u8] {
        &self.bytes[..8]
    }
    pub fn get_timestamp_bytes(&self) -> &[u8] {
        &self.bytes[9..17]
    }
    pub fn get_checksum_bytes(&self) -> &[u8] {
        &self.bytes[33..37]
    }
    pub fn get_headers_bytes(&self) -> &[u8] {
        let headers_length = u32::from_le_bytes(self.bytes[37..41].try_into().unwrap());
        &self.bytes[41..41 + headers_length as usize]
    }
    pub fn get_message_state_byte(&self) -> &u8 {
        &self.bytes[8]
    }
    pub fn get_payload_bytes(&self) -> &[u8] {
        let headers_length = u32::from_le_bytes(self.bytes[37..41].try_into().unwrap());
        let payload_length = u32::from_le_bytes(
            self.bytes[41 + headers_length as usize..41 + headers_length as usize + 4]
                .try_into()
                .unwrap(),
        );
        &self.bytes[41 + headers_length as usize + 4..payload_length as usize]
    }
}

impl Sizeable for Arc<RetainedMessage> {
    fn get_size_bytes(&self) -> u32 {
        24 + self.length
    }
}
impl Sizeable for RetainedMessage {
    fn get_size_bytes(&self) -> u32 {
        4 + self.bytes.len() as u32
    }
}

/// The state of the message, currently only the `Available` state is used.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageState {
    /// The message is available.
    Available,
    /// The message is unavailable.
    Unavailable,
    /// The message is poisoned.
    Poisoned,
    /// The message is marked for deletion.
    MarkedForDeletion,
}

impl MessageState {
    /// Returns the code of the message state.
    pub fn as_code(&self) -> u8 {
        match self {
            MessageState::Available => 1,
            MessageState::Unavailable => 10,
            MessageState::Poisoned => 20,
            MessageState::MarkedForDeletion => 30,
        }
    }

    /// Returns the message state from the code.
    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(MessageState::Available),
            10 => Ok(MessageState::Unavailable),
            20 => Ok(MessageState::Poisoned),
            30 => Ok(MessageState::MarkedForDeletion),
            _ => Err(IggyError::InvalidCommand),
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
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "available" => Ok(MessageState::Available),
            "unavailable" => Ok(MessageState::Unavailable),
            "poisoned" => Ok(MessageState::Poisoned),
            "marked_for_deletion" => Ok(MessageState::MarkedForDeletion),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl Sizeable for Arc<PolledMessage> {
    fn get_size_bytes(&self) -> u32 {
        self.as_ref().get_size_bytes()
    }
}

impl PolledMessage {
    /// Creates a new message with a specified offset.
    pub fn create(
        offset: u64,
        state: MessageState,
        timestamp: u64,
        id: u128,
        payload: Bytes,
        checksum: u32,
        headers: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> Self {
        PolledMessage {
            offset,
            state,
            timestamp,
            id,
            checksum,
            #[allow(clippy::cast_possible_truncation)]
            length: payload.len() as u32,
            payload,
            headers,
        }
    }
    /// Returns the size of the message in bytes.
    pub fn get_size_bytes(&self) -> u32 {
        // Offset + State + Timestamp + ID + Checksum + Length + Payload + Headers
        POLLED_MESSAGE_METADATA + self.length + header::get_headers_size_bytes(&self.headers)
    }
}
