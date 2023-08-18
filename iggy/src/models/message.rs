use crate::header;
use crate::header::{HeaderKey, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub offset: u64,
    pub timestamp: u64,
    pub id: u128,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
    #[serde(skip)]
    pub length: u32,
    #[serde_as(as = "Base64")]
    pub payload: Vec<u8>,
}

impl Message {
    pub fn get_size_bytes(&self) -> u32 {
        // Offset + Timestamp + ID + Headers + Length + Payload
        8 + 8 + 16 + header::get_headers_size_bytes(&self.headers) + 4 + self.payload.len() as u32
    }
}
