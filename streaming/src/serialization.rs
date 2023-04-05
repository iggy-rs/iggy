#[derive(Debug)]
pub struct SerializedMessage {
    pub offset: u64,
    pub timestamp: u64,
    pub crc: u64,
    pub length: u64,
    pub body: Vec<u8>
}