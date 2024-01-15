use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use crate::messages::send_messages;
use crate::models::header;
use crate::models::header::{HeaderKey, HeaderValue};
use crate::sizeable::Sizeable;
use crate::utils::{checksum, timestamp::IggyTimestamp};
use bytes::{BufMut, Bytes};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;

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
    pub messages: Vec<Message>,
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
pub struct Message {
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

impl Sizeable for Arc<Message> {
    fn get_size_bytes(&self) -> u32 {
        self.as_ref().get_size_bytes()
    }
}

impl Message {
    /// Creates a new message from the `Message` struct being part of `SendMessages` command.
    pub fn from_message(message: &send_messages::Message) -> Self {
        let timestamp = IggyTimestamp::now().to_micros();
        let checksum = checksum::calculate(&message.payload);
        let headers = message.headers.as_ref().cloned();

        Self::empty(
            timestamp,
            MessageState::Available,
            message.id,
            message.payload.clone(),
            checksum,
            headers,
        )
    }

    /// Creates a new message without a specified offset.
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
        Message {
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
        8 + 1 + 8 + 16 + 4 + 4 + self.length + header::get_headers_size_bytes(&self.headers)
    }

    /// Extends the provided bytes with the message.
    pub fn extend(&self, bytes: &mut Vec<u8>) {
        bytes.put_u64_le(self.offset);
        bytes.put_u8(self.state.as_code());
        bytes.put_u64_le(self.timestamp);
        bytes.put_u128_le(self.id);
        bytes.put_u32_le(self.checksum);
        if let Some(headers) = &self.headers {
            let headers_bytes = headers.as_bytes();
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u32_le(headers_bytes.len() as u32);
            bytes.extend(&headers_bytes);
        } else {
            bytes.put_u32_le(0u32);
        }
        bytes.put_u32_le(self.length);
        bytes.extend(&self.payload);
    }
}
