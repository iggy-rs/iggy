use super::byte_size::IggyByteSize;

/// Trait for types that return their size in bytes.
pub trait Sizeable {
    fn get_size_bytes(&self) -> IggyByteSize;
}
