#[derive(Debug)]
pub struct Message {
    pub offset: u64,
    pub timestamp: u64,
    pub body: Vec<u8>,
}

impl Message {
    pub fn create(body: Vec<u8>) -> Self {
        Message {
            offset: 0,
            timestamp: 0,
            body,
        }
    }
}
