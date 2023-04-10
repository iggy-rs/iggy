#[derive(Debug)]
pub struct Message {
    pub offset: u64,
    pub timestamp: u64,
    pub length: u64,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn new(payload: Vec<u8>) -> Self {
        Message::create(0, 0, payload)
    }

    pub fn create(offset: u64, timestamp: u64, payload: Vec<u8>) -> Self {
        Message {
            offset,
            timestamp,
            length: payload.len() as u64,
            payload,
        }
    }

    pub fn get_size_bytes(&self) -> u64 {
        // Offset + Timestamp + Length + Payload
        8 + 8 + 8 + self.payload.len() as u64
    }
}
