/// Trait for types that return their size in bytes.
pub trait Sizeable {
    fn get_size_bytes(&self) -> u32;
}
