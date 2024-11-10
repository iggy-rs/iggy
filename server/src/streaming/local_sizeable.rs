use iggy::utils::byte_size::IggyByteSize;

/// Trait for types that return their size in bytes.
/// repeated from the sdk because of the coherence rule, see messages.rs
pub trait LocalSizeable {
    fn get_size_bytes(&self) -> IggyByteSize;
}
