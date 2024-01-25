use crate::batching::batcher::Itemizer;
use crate::error::IggyError;

pub trait BatchesFilter<M, U: Itemizer<M>, T: IntoIterator<Item = U>> {
    fn filter_by_offset_range(
        self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<M>, IggyError>;
}
