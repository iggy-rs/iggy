use crate::utils::{checksum, timestamp};
use bytes::Bytes;
use iggy::messages::send_messages;
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub offset: u64,
    pub timestamp: u64,
    pub id: u128,
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
        Self::empty(timestamp, message.id, message.payload.clone(), checksum)
    }

    pub fn empty(timestamp: u64, id: u128, payload: Bytes, checksum: u32) -> Self {
        Message::create(0, timestamp, id, payload, checksum)
    }

    pub fn create(offset: u64, timestamp: u64, id: u128, payload: Bytes, checksum: u32) -> Self {
        Message {
            offset,
            timestamp,
            id,
            checksum,
            length: payload.len() as u32,
            payload,
        }
    }

    pub fn get_size_bytes(&self, with_checksum: bool) -> u32 {
        // Offset + Timestamp + ID + Length + Payload
        let size = 8 + 8 + 16 + 4 + self.length;
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
        bytes.extend(self.length.to_le_bytes());
        bytes.extend(&self.payload);
    }
}
