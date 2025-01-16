use crate::messages::send_messages::Message;
use crate::models::header;
use crate::models::header::{HeaderKey, HeaderValue};
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;

pub const IGGY_MESSAGE_METADATA: u32 = 8 + 1 + 8 + 4;

/// The single message that is polled from the partition.
/// It consists of the following fields:
/// - `id`: the identifier of the message.
/// - `headers`: the optional headers of the message.
/// - `length`: the length of the payload.
/// - `payload`: the binary payload of the message.
#[serde_as]
#[derive(
    Debug, serde::Serialize, serde::Deserialize, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive,
)]
#[rkyv(derive(Debug))]
pub struct IggyMessage {
    /// The identifier of the message.
    pub id: u128,
    /// The offset of the message.
    offset_delta: u32,
    /// The timestamp of the message.
    timestamp_delta: u32,
    /// The length of the payload.
    #[serde(skip)]
    pub length: u64,
    /// The binary payload of the message.
    #[serde_as(as = "Base64")]
    pub payload: Vec<u8>,
    /// The optional headers of the message.
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
}

impl From<Message> for IggyMessage {
    fn from(value: Message) -> Self {
        IggyMessage::create(value.id, value.payload, value.headers)
    }
}

impl IggyMessage {
    /// Creates a new message with a specified offset.
    pub fn create(
        id: u128,
        payload: Vec<u8>,
        headers: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> Self {
        IggyMessage {
            id,
            length: payload.len() as u64,
            offset_delta: 0,
            timestamp_delta: 0,
            payload,
            headers,
        }
    }
}

impl std::fmt::Display for IggyMessage {
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

impl Sizeable for IggyMessage {
    fn get_size_bytes(&self) -> IggyByteSize {
        // id + offset_delta + timestamp_delta + length + payload + headers
        let value = header::get_headers_size_bytes(&self.headers).as_bytes_u64()
            + self.length
            + 16
            + 4
            + 4
            + 8;
        value.into()
    }
}
