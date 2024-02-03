use crate::error::IggyError;
// Making those traits generic , in case if in the future
// there would be more message formats
pub trait Batcher<T, U: BatchItemizer<T>>: IntoIterator<Item = T> {
    fn into_batch(
        self,
        base_offset: u64,
        last_offset_delta: u32,
        attributes: u8,
    ) -> Result<U, IggyError>;
}
pub trait BatchItemizer<T> {
    fn into_messages(self) -> Result<impl IntoIterator<Item = T>, IggyError>;
}
