use crate::error::Error;

pub trait BytesSerializable {
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self, Error>
    where
        Self: Sized;
}
