use crate::streaming::batching::iterator::IntoMessagesIterator;

pub trait BatchItemizer<M, U: IntoMessagesIterator, T: IntoIterator<Item = U>> {
    fn convert_and_filter_by_offset_range(self, start_offset: u64, end_offset: u64) -> Vec<M>;
}
