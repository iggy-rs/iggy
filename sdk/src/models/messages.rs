use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::messages::send_messages::Message;
use crate::models::header;
use crate::models::header::{HeaderKey, HeaderValue};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

pub const POLLED_MESSAGE_METADATA: u32 = 8 + 1 + 8 + 4;

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

impl PolledMessage {
    /// Creates a new message from the `Message` struct being part of `SendMessages` command.
    /// Creates a new message without a specified offset.
    pub fn empty(
        timestamp: u64,
        state: MessageState,
        id: u128,
        payload: Bytes,
        checksum: u32,
        headers: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> Self {
        PolledMessage::create(0, state, timestamp, id, payload, checksum, headers)
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
    pub fn try_from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        let offset = u64::from_le_bytes(bytes[..8].try_into()?);
        let message_state = MessageState::from_code(bytes[8])?;
        let timestamp = u64::from_le_bytes(bytes[9..17].try_into()?);
        let id = u128::from_le_bytes(bytes[17..33].try_into()?);
        let checksum = u32::from_le_bytes(bytes[33..37].try_into()?);
        let headers_length = u32::from_le_bytes(bytes[37..41].try_into()?);
        let headers = if headers_length > 0 {
            Some(HashMap::from_bytes(
                bytes.slice(41..headers_length as usize),
            )?)
        } else {
            None
        };
        let position = 41 + headers_length as usize;
        let length = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let payload = Bytes::from(bytes.slice(position + 4..position + 4 + length as usize));

        Ok(PolledMessage {
            offset,
            timestamp,
            checksum,
            state: message_state,
            id,
            headers,
            length,
            payload,
        })
    }

    /// Returns the size of the message in bytes.
    pub fn get_size_bytes(&self) -> u32 {
        // Offset + State + Timestamp + ID + Checksum + Length + Payload + Headers
        POLLED_MESSAGE_METADATA + self.length + header::get_headers_size_bytes(&self.headers)
    }
}
