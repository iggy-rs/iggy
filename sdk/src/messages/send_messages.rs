use crate::bytes_serializable::BytesSerializable;
use crate::command::{Command, SEND_MESSAGES_CODE};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::messages::{MAX_HEADERS_SIZE, MAX_PAYLOAD_SIZE};
use crate::models::batch::IggyBatch;
use crate::models::header;
use crate::models::header::{HeaderKey, HeaderValue};
use crate::models::messages::IggyMessage;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use crate::utils::val_align_up;
use crate::validatable::Validatable;
use bytes::{BufMut, Bytes, BytesMut};
use rkyv::util::AlignedVec;
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::str::FromStr;

const EMPTY_KEY_VALUE: Vec<u8> = vec![];

/// `SendMessages` command is used to send messages to a topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `partitioning` - to which partition the messages should be sent - either provided by the client or calculated by the server.
/// - `batch` - batch of messages to be sent.
#[derive(Debug, Serialize, Deserialize)]
pub struct SendMessages {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// To which partition the messages should be sent - either provided by the client or calculated by the server.
    pub partitioning: Partitioning,
    /// Batch of messages to be sent.
    pub batch: IggyBatch,
}

/// `Partitioning` is used to specify to which partition the messages should be sent.
/// It has the following kinds:
/// - `Balanced` - the partition ID is calculated by the server using the round-robin algorithm.
/// - `PartitionId` - the partition ID is provided by the client.
/// - `MessagesKey` - the partition ID is calculated by the server using the hash of the provided messages key.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Partitioning {
    /// The kind of partitioning.
    pub kind: PartitioningKind,
    #[serde(skip)]
    /// The length of the value payload.
    pub length: u8,
    #[serde_as(as = "Base64")]
    /// The binary value payload.
    pub value: Vec<u8>,
}

impl Hash for Partitioning {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.length.hash(state);
        self.value.hash(state);
    }
}

// TODO!:  We can get rid of this aswell, and use `IggyMessage` struct with hidden unitialized fields..

/// The single message to be sent. It has the following payload:
/// - `id` - unique message ID, if not specified by the client (has value = 0), it will be generated by the server.
/// - `length` - length of the payload.
/// - `payload` - binary message payload.
/// - `headers` - optional collection of headers.
#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Message {
    /// Unique message ID, if not specified by the client (has value = 0), it will be generated by the server.
    #[serde(default = "default_message_id")]
    pub id: u128,
    #[serde(skip)]
    /// Length of the payload.
    pub length: u32,
    #[serde_as(as = "Base64")]
    /// Binary message payload.
    pub payload: Vec<u8>,
    /// Optional collection of headers.
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
}

/// `PartitioningKind` is an enum which specifies the kind of partitioning and is used by `Partitioning`.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PartitioningKind {
    /// The partition ID is calculated by the server using the round-robin algorithm.
    #[default]
    Balanced,
    /// The partition ID is provided by the client.
    PartitionId,
    /// The partition ID is calculated by the server using the hash of the provided messages key.
    MessagesKey,
}

impl Hash for PartitioningKind {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_code().hash(state);
    }
}

fn default_message_id() -> u128 {
    0
}

impl Default for SendMessages {
    fn default() -> Self {
        SendMessages {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partitioning: Partitioning::default(),
            batch: Default::default(),
        }
    }
}

impl Default for Partitioning {
    fn default() -> Self {
        Partitioning::balanced()
    }
}

impl Partitioning {
    /// Partition the messages using the balanced round-robin algorithm on the server.
    pub fn balanced() -> Self {
        Partitioning {
            kind: PartitioningKind::Balanced,
            length: 0,
            value: EMPTY_KEY_VALUE,
        }
    }

    /// Partition the messages using the provided partition ID.
    pub fn partition_id(partition_id: u32) -> Self {
        Partitioning {
            kind: PartitioningKind::PartitionId,
            length: 4,
            value: partition_id.to_le_bytes().to_vec(),
        }
    }

    /// Partition the messages using the provided messages key.
    pub fn messages_key(value: &[u8]) -> Result<Self, IggyError> {
        let length = value.len();
        if length == 0 || length > 255 {
            return Err(IggyError::InvalidCommand);
        }

        Ok(Partitioning {
            kind: PartitioningKind::MessagesKey,
            #[allow(clippy::cast_possible_truncation)]
            length: length as u8,
            value: value.to_vec(),
        })
    }

    /// Partition the messages using the provided messages key as str.
    pub fn messages_key_str(value: &str) -> Result<Self, IggyError> {
        Self::messages_key(value.as_bytes())
    }

    /// Partition the messages using the provided messages key as u32.
    pub fn messages_key_u32(value: u32) -> Self {
        Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: 4,
            value: value.to_le_bytes().to_vec(),
        }
    }

    /// Partition the messages using the provided messages key as u64.
    pub fn messages_key_u64(value: u64) -> Self {
        Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: 8,
            value: value.to_le_bytes().to_vec(),
        }
    }

    /// Partition the messages using the provided messages key as u128.
    pub fn messages_key_u128(value: u128) -> Self {
        Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: 16,
            value: value.to_le_bytes().to_vec(),
        }
    }

    /// Create the partitioning from the provided partitioning.
    pub fn from_partitioning(partitioning: &Partitioning) -> Self {
        Partitioning {
            kind: partitioning.kind,
            length: partitioning.length,
            value: partitioning.value.clone(),
        }
    }
}

impl Sizeable for Partitioning {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(u64::from(self.length) + 2)
    }
}

impl Command for SendMessages {
    fn code(&self) -> u32 {
        SEND_MESSAGES_CODE
    }
}

impl BytesSerializable for SendMessages {
    fn to_bytes(&self) -> Bytes {
        todo!()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl Validatable<IggyError> for SendMessages {
    fn validate(&self) -> Result<(), IggyError> {
        if self.batch.messages.is_empty() {
            return Err(IggyError::InvalidMessagesCount);
        }

        let key_value_length = self.partitioning.value.len();
        if key_value_length > 255
            || (self.partitioning.kind != PartitioningKind::Balanced && key_value_length == 0)
        {
            return Err(IggyError::InvalidKeyValueLength);
        }

        let mut headers_size = 0;
        let mut payload_size = 0;
        for message in &self.batch.messages {
            if let Some(headers) = &message.headers {
                for value in headers.values() {
                    headers_size += value.value.len() as u32;
                    if headers_size > MAX_HEADERS_SIZE {
                        return Err(IggyError::TooBigHeadersPayload);
                    }
                }
            }
            payload_size += message.payload.len() as u32;
            if payload_size > MAX_PAYLOAD_SIZE {
                return Err(IggyError::TooBigMessagePayload);
            }
        }

        if payload_size == 0 {
            return Err(IggyError::EmptyMessagePayload);
        }

        Ok(())
    }
}

impl PartitioningKind {
    /// Get the code of the partitioning kind.
    pub fn as_code(&self) -> u8 {
        match self {
            PartitioningKind::Balanced => 1,
            PartitioningKind::PartitionId => 2,
            PartitioningKind::MessagesKey => 3,
        }
    }

    /// Get the partitioning kind from the provided code.
    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(PartitioningKind::Balanced),
            2 => Ok(PartitioningKind::PartitionId),
            3 => Ok(PartitioningKind::MessagesKey),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl Message {
    /// Create a new message with the optional ID, payload and headers.
    pub fn new(
        id: Option<u128>,
        payload: Vec<u8>,
        headers: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> Self {
        Message {
            id: id.unwrap_or(0),
            #[allow(clippy::cast_possible_truncation)]
            length: payload.len() as u32,
            payload,
            headers,
        }
    }
}

impl Sizeable for Message {
    fn get_size_bytes(&self) -> IggyByteSize {
        // ID + Length + Payload + Headers
        header::get_headers_size_bytes(&self.headers) + (16 + 4 + self.payload.len() as u64).into()
    }
}

impl Default for Message {
    fn default() -> Self {
        let payload = b"hello world".to_vec();
        Message {
            id: 1,
            length: payload.len() as u32,
            payload,
            headers: None,
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.payload.len();

        if len > 40 {
            write!(
                f,
                "{}|{}...{}",
                self.id,
                String::from_utf8_lossy(&self.payload[..20]),
                String::from_utf8_lossy(&self.payload[len - 20..])
            )
        } else {
            write!(f, "{}|{}", self.id, String::from_utf8_lossy(&self.payload))
        }
    }
}

impl Partitioning {
    pub fn from_bytes_new(bytes: &[u8]) -> Result<(usize, Self), IggyError> {
        let kind = PartitioningKind::from_code(bytes[0])?;
        let length = bytes[1];
        // Hmm this one is called on hot path, could we somehow optimize away this small allocation ?
        let value = bytes[2..2 + length as usize].to_vec();
        if value.len() != length as usize {
            return Err(IggyError::InvalidCommand);
        }
        let read = 1 + 1 + length as usize;

        Ok((
            read,
            Partitioning {
                kind,
                length,
                value,
            },
        ))
    }
}

impl BytesSerializable for Partitioning {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(2 + self.length as usize);
        bytes.put_u8(self.kind.as_code());
        bytes.put_u8(self.length);
        bytes.put_slice(&self.value);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        if bytes.len() < 3 {
            return Err(IggyError::InvalidCommand);
        }

        let kind = PartitioningKind::from_code(bytes[0])?;
        let length = bytes[1];
        let value = bytes[2..2 + length as usize].to_vec();
        if value.len() != length as usize {
            return Err(IggyError::InvalidCommand);
        }

        Ok(Partitioning {
            kind,
            length,
            value,
        })
    }
}

// This method is used by the new version of `IggyClient` to serialize `SendMessages` without copying the messages.
pub(crate) fn as_bytes(
    stream_id: &Identifier,
    topic_id: &Identifier,
    partitioning: &Partitioning,
    batch: &IggyBatch,
) -> AlignedVec<512> {
    let messages_size = batch
        .messages
        .iter()
        .map(IggyMessage::get_size_bytes)
        .sum::<IggyByteSize>();
    // shit ton of small allocations, can we move it to stack ?
    let key_bytes = partitioning.to_bytes();
    let stream_id_bytes = stream_id.to_bytes();
    let topic_id_bytes = topic_id.to_bytes();
    let total_size = messages_size.as_bytes_usize()
        + key_bytes.len()
        + stream_id_bytes.len()
        + topic_id_bytes.len();
    let total_size = val_align_up(total_size as _, 512);

    let mut bytes = AlignedVec::<512>::with_capacity(total_size as _);
    // TODO: create abstraction over this shit.
    unsafe { bytes.set_len(total_size as _) };
    let mut bytes =
        rkyv::api::high::to_bytes_in::<AlignedVec<512>, rkyv::rancor::Error>(batch, bytes)
            .expect("Failed to serialize batcherino");

    // The idea is to use the padding required by reykjavik, to fit the metadata about request.
    let mut position = 0;
    bytes[position..position + key_bytes.len()].copy_from_slice(&key_bytes);
    position += key_bytes.len();
    bytes[position..position + stream_id_bytes.len()].copy_from_slice(&stream_id_bytes);
    position += stream_id_bytes.len();
    bytes[position..position + topic_id_bytes.len()].copy_from_slice(&topic_id_bytes);

    bytes
}

impl FromStr for Message {
    type Err = IggyError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let id = default_message_id();
        let payload = input.as_bytes().to_vec();
        let length = payload.len() as u32;
        if length == 0 {
            return Err(IggyError::EmptyMessagePayload);
        }

        Ok(Message {
            id,
            length,
            payload,
            headers: None,
        })
    }
}

impl SendMessages {
    fn to_bytes(&self) -> AlignedVec<512> {
        as_bytes(
            &self.stream_id,
            &self.topic_id,
            &self.partitioning,
            &self.batch,
        )
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<SendMessages, IggyError> {
        if bytes.len() < 11 {
            return Err(IggyError::InvalidCommand);
        }
        todo!();
    }
}

impl Display for SendMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.stream_id,
            self.topic_id,
            self.partitioning,
            self.batch
                .messages
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<String>>()
                .join("|")
        )
    }
}

impl Display for Partitioning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            PartitioningKind::Balanced => write!(f, "{}|0", self.kind),
            PartitioningKind::PartitionId => write!(
                f,
                "{}|{}",
                self.kind,
                u32::from_le_bytes(self.value[..4].try_into().unwrap())
            ),
            PartitioningKind::MessagesKey => {
                write!(f, "{}|{}", self.kind, String::from_utf8_lossy(&self.value))
            }
        }
    }
}

impl Display for PartitioningKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitioningKind::Balanced => write!(f, "balanced"),
            PartitioningKind::PartitionId => write!(f, "partition_id"),
            PartitioningKind::MessagesKey => write!(f, "messages_key"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        todo!();
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        todo!();
    }

    #[test]
    fn key_of_type_balanced_should_have_empty_value() {
        let key = Partitioning::balanced();
        assert_eq!(key.kind, PartitioningKind::Balanced);
        assert_eq!(key.length, 0);
        assert_eq!(key.value, EMPTY_KEY_VALUE);
        assert_eq!(
            PartitioningKind::from_code(1).unwrap(),
            PartitioningKind::Balanced
        );
    }

    #[test]
    fn key_of_type_partition_should_have_value_of_const_length_4() {
        let partition_id = 1234u32;
        let key = Partitioning::partition_id(partition_id);
        assert_eq!(key.kind, PartitioningKind::PartitionId);
        assert_eq!(key.length, 4);
        assert_eq!(key.value, partition_id.to_le_bytes());
        assert_eq!(
            PartitioningKind::from_code(2).unwrap(),
            PartitioningKind::PartitionId
        );
    }

    #[test]
    fn key_of_type_messages_key_should_have_value_of_dynamic_length() {
        let messages_key = "hello world";
        let key = Partitioning::messages_key_str(messages_key).unwrap();
        assert_eq!(key.kind, PartitioningKind::MessagesKey);
        assert_eq!(key.length, messages_key.len() as u8);
        assert_eq!(key.value, messages_key.as_bytes());
        assert_eq!(
            PartitioningKind::from_code(3).unwrap(),
            PartitioningKind::MessagesKey
        );
    }

    #[test]
    fn key_of_type_messages_key_that_has_length_0_should_fail() {
        let messages_key = "";
        let key = Partitioning::messages_key_str(messages_key);
        assert!(key.is_err());
    }

    #[test]
    fn key_of_type_messages_key_that_has_length_greater_than_255_should_fail() {
        let messages_key = "a".repeat(256);
        let key = Partitioning::messages_key_str(&messages_key);
        assert!(key.is_err());
    }
}
