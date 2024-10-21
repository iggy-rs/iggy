use bytes::Bytes;
use super::header::Header;

pub struct IggyMessage {
    header: Header,
    body: Bytes,
}
