use super::header::Header;
use bytes::Bytes;

pub struct IggyMessage {
    pub header: Header,
    // Certain messages won't have an body.
    // TODO: Once we have a proper enum for the message body, replace `Bytes` with it.
    pub body: Bytes,
}

impl IggyMessage {
    pub fn new(header: Header, body: Bytes) -> Self {
        Self { header, body }
    }
}
