use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::messages::send_messages;
use crate::models::header;
use crate::models::header::{HeaderKey, HeaderValue};
use crate::sizeable::Sizeable;
use crate::utils::{checksum, timestamp::IggyTimestamp};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{BufMut, Bytes, BytesMut};
use serde::ser::SerializeStruct;
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

impl From<RetainedMessage> for Message {
    fn from(value: RetainedMessage) -> Self {
        let offset = u64::from_le_bytes(value.payload[..8].try_into().unwrap());
        let state = MessageState::from_code(value.payload[8]).unwrap();
        let timestamp = u64::from_le_bytes(value.payload[9..17].try_into().unwrap());
        let id = u128::from_le_bytes(value.payload[17..33].try_into().unwrap());
        let checksum = u32::from_le_bytes(value.payload[33..37].try_into().unwrap());
        let headers_length = u32::from_le_bytes(value.payload[37..41].try_into().unwrap());
        let headers = match headers_length {
            0 => None,
            _ => {
                let headers_payload = value.payload[41..41 + headers_length as usize].to_owned();
                let headers = HashMap::from_bytes(Bytes::from(headers_payload)).unwrap();
                Some(headers)
            }
        };
        let payload_length = u32::from_le_bytes(
            value.payload[41 + headers_length as usize..41 + headers_length as usize + 4]
                .try_into()
                .unwrap(),
        );

        Message::create(
            offset,
            state,
            timestamp,
            id,
            value.payload.slice(
                41 + headers_length as usize + 4
                    ..41 + headers_length as usize + 4 + payload_length as usize,
            ),
            checksum,
            headers,
        )
    }
}

#[derive(Debug, Clone)]
pub struct RetainedMessage {
    pub length: u32,
    pub payload: Bytes,
}

impl Serialize for RetainedMessage {
    fn serialize<S>(&self, serializer: S) -> Result<<S as serde::Serializer>::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let msg = Message::from(self.clone());
        msg.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for RetainedMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let msg = Message::deserialize(deserializer)?;
        Ok(msg.into())
    }
}

impl From<Message> for RetainedMessage {
    fn from(value: Message) -> Self {
        let mut payload = BytesMut::with_capacity(
            4 + 1
                + 8
                + 8
                + 16
                + 4
                + 4
                + value.length as usize
                + header::get_headers_size_bytes(&value.headers) as usize,
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
            length: payload.len() as u32,
            payload: payload.freeze(),
        }
    }
}

impl RetainedMessage {
    pub fn new(length: u32, payload: Bytes) -> Self {
        Self { length, payload }
    }

    pub fn from_bytes(
        id_bytes: &[u8],
        offset_bytes: &[u8],
        timestamp_bytes: &[u8],
        message_state_byte: &u8,
        checksum_bytes: &[u8],
        headers_bytes: &[u8],
        payload_bytes: &[u8],
    ) -> Self {
        let mut payload = BytesMut::with_capacity(
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
            length: payload.len() as u32,
            payload: payload.freeze(),
        }
    }

    pub fn from_message(offset: u64, message: send_messages::Message) -> Self {
        // Can use with_capacity, if would to move headers to bytes instead of HashMap<HeaderKey, HeaderValue>
        let mut payload = BytesMut::new();

        let timestamp = IggyTimestamp::now().to_micros();
        let checksum = checksum::calculate(&message.payload);
        let message_state = MessageState::Available;
        let length = message.payload.len() as u32;

        payload.put_u64_le(offset);
        payload.put_u8(message_state.as_code());
        payload.put_u64_le(timestamp);
        payload.put_u128_le(message.id);
        payload.put_u32_le(checksum);
        if let Some(headers) = message.headers {
            let headers_bytes = headers.as_bytes();
            #[allow(clippy::cast_possible_truncation)]
            payload.put_u32_le(headers_bytes.len() as u32);
            payload.put_slice(&headers_bytes);
        } else {
            payload.put_u32_le(0u32);
        }
        payload.put_u32_le(length);
        payload.put_slice(&message.payload);

        Self {
            length: payload.len() as u32,
            payload: payload.freeze(),
        }
    }

    pub fn get_id(&self) -> u128 {
        u128::from_le_bytes(self.payload[17..33].try_into().unwrap())
    }
    pub fn get_message_state(&self) -> MessageState {
        MessageState::from_code(self.payload[8]).unwrap()
    }
    pub fn get_offset(&self) -> u64 {
        u64::from_le_bytes(self.payload[..8].try_into().unwrap())
    }
    pub fn get_timestamp(&self) -> u64 {
        u64::from_le_bytes(self.payload[9..17].try_into().unwrap())
    }
    pub fn get_checksum(&self) -> u32 {
        u32::from_le_bytes(self.payload[33..37].try_into().unwrap())
    }
    pub fn try_get_headers(&self) -> Result<Option<HashMap<HeaderKey, HeaderValue>>, IggyError> {
        let headers_length = u32::from_le_bytes(self.payload[37..41].try_into().unwrap());
        if headers_length == 0 {
            return Ok(None);
        }
        let headers_payload = self.payload[41..41 + headers_length as usize].to_owned();
        let headers_bytes = Bytes::from(headers_payload);
        Ok(Some(HashMap::from_bytes(headers_bytes)?))
    }
    pub fn get_payload(&self) -> &[u8] {
        let headers_length = u32::from_le_bytes(self.payload[37..41].try_into().unwrap());
        let payload_length = u32::from_le_bytes(
            self.payload[41 + headers_length as usize..41 + headers_length as usize + 4]
                .try_into()
                .unwrap(),
        );
        &self.payload[41 + headers_length as usize + 4..payload_length as usize]
    }
    pub fn get_id_bytes(&self) -> &[u8] {
        &self.payload[17..33]
    }
    pub fn get_offset_bytes(&self) -> &[u8] {
        &self.payload[..8]
    }
    pub fn get_timestamp_bytes(&self) -> &[u8] {
        &self.payload[9..17]
    }
    pub fn get_checksum_bytes(&self) -> &[u8] {
        &self.payload[33..37]
    }
    pub fn get_headers_bytes(&self) -> &[u8] {
        let headers_length = u32::from_le_bytes(self.payload[37..41].try_into().unwrap());
        &self.payload[41..41 + headers_length as usize]
    }
    pub fn get_message_state_byte(&self) -> &u8 {
        &self.payload[8]
    }
    pub fn get_payload_bytes(&self) -> &[u8] {
        let headers_length = u32::from_le_bytes(self.payload[37..41].try_into().unwrap());
        let payload_length = u32::from_le_bytes(
            self.payload[41 + headers_length as usize..41 + headers_length as usize + 4]
                .try_into()
                .unwrap(),
        );
        &self.payload[41 + headers_length as usize + 4..payload_length as usize]
    }
}

impl Sizeable for Arc<RetainedMessage> {
    fn get_size_bytes(&self) -> u32 {
        4 + self.payload.len() as u32
    }
}
impl Sizeable for RetainedMessage {
    fn get_size_bytes(&self) -> u32 {
        4 + self.payload.len() as u32
    }
}

impl Sizeable for &Arc<RetainedMessage> {
    fn get_size_bytes(&self) -> u32 {
        4 + self.payload.len() as u32
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
    pub fn extend(&self, bytes: &mut BytesMut) {
        bytes.put_u64_le(self.offset);
        bytes.put_u8(self.state.as_code());
        bytes.put_u64_le(self.timestamp);
        bytes.put_u128_le(self.id);
        bytes.put_u32_le(self.checksum);
        if let Some(headers) = &self.headers {
            let headers_bytes = headers.as_bytes();
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u32_le(headers_bytes.len() as u32);
            bytes.put_slice(&headers_bytes);
        } else {
            bytes.put_u32_le(0u32);
        }
        bytes.put_u32_le(self.length);
        bytes.put_slice(&self.payload);
    }
}
impl BytesSerializable for Message {
    fn as_bytes(&self) -> Bytes {
        let size = self.get_size_bytes() as usize;
        let mut buffer = BytesMut::with_capacity(size);
        self.extend(&mut buffer);
        buffer.freeze()
    }
    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let offset = u64::from_le_bytes(bytes[..8].try_into()?);
        let state_code = MessageState::from_code(bytes[8])?;
        let timestamp = u64::from_le_bytes(bytes[9..17].try_into()?);
        let id = u128::from_le_bytes(bytes[17..33].try_into()?);
        let checksum = u32::from_le_bytes(bytes[33..37].try_into()?);

        let headers_length = u32::from_le_bytes(bytes[37..41].try_into()?);
        let headers = match headers_length {
            0 => None,
            _ => {
                let headers_payload = bytes[41..41 + headers_length as usize].to_owned();
                let headers = HashMap::from_bytes(Bytes::from(headers_payload))?;
                Some(headers)
            }
        };

        let payload_length = u32::from_le_bytes(bytes[41 + headers_length as usize..].try_into()?);
        let payload = vec![0; payload_length as usize];
        Ok(Self::create(
            offset,
            state_code,
            timestamp,
            id,
            Bytes::from(payload),
            checksum,
            headers,
        ))
    }
}
