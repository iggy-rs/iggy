use iggy::utils::byte_size::IggyByteSize;

/// Trait for types that return their size in bytes.
/// repeated from the sdk because of the coherence rule, see messages.rs
pub trait LocalSizeable {
    fn get_size_bytes(&self) -> IggyByteSize;
}

/// Trait for calculating the real memory size of a type, including all its fields
/// and any additional overhead from containers like Arc, Vec, etc.
pub trait RealSize {
    fn real_size(&self) -> IggyByteSize;
}
