use crate::error::Error;

pub trait BytesSerializable {
    type Type;
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, Error>;
}
