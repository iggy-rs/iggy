#[derive(Debug)]
pub struct Message {
    pub offset: u64,
    pub timestamp: u64,
    pub length: u32,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn get_size_bytes(&self) -> u32 {
        // Offset + Timestamp + Length + Payload
        8 + 8 + 4 + self.payload.len() as u32
    }
}