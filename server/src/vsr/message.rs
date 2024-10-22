use bytes::Bytes;
use super::header::Header;

pub struct IggyMessage {
    pub header: Header,
    // Certain messages won't have an body.
    // TODO: Once we have a proper enum for the message body, replace `Bytes` with it.
    pub body: Bytes,
}
