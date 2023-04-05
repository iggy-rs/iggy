#[derive(Debug)]
pub struct Message {
    pub offset: u64,
    pub timestamp: u64,
    pub length: u64,
    pub payload: String
}