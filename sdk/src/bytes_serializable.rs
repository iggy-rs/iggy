use crate::error::IggyError;

/// The trait represents the logic responsible for serializing and deserializing the struct to and from bytes.
pub trait BytesSerializable {
    /// Serializes the struct to bytes.
    fn as_bytes(&self) -> Vec<u8>;

    /// Deserializes the struct from bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self, IggyError>
    where
        Self: Sized;
}
