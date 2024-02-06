use bytes::Bytes;

use crate::error::IggyError;

/// The trait represents the logic responsible for serializing and deserializing the struct to and from bytes.
pub trait BytesSerializable {
    /// Serializes the struct to bytes.
    fn as_bytes(&self) -> Bytes;

    /// Deserializes the struct from bytes.
    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized;

    /// Computes the size of the struct in bytes.
    fn size(&self) -> usize {
        unimplemented!("size")
    }
}
