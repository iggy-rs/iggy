use crate::error::Error;
// Making those traits generic , in case if in the future
// there would be more message formats
pub trait Batcher<T, U: Itemizer<T>>: IntoIterator<Item = T> {
    fn into_batch(
        self,
        base_offset: u64,
        last_offset_delta: u32,
        attributes: u8,
    ) -> Result<U, Error>;
}
pub trait Itemizer<T> {
    fn into_messages(self) -> Result<impl IntoIterator<Item = T>, Error>;
}
