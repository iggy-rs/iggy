use crate::batching::batcher::BatchItemizer;
use crate::error::IggyError;

pub trait BatchesConverter<M, U: BatchItemizer<M>, T: IntoIterator<Item = U>> {
    fn convert_and_filter_by_offset_range(
        self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<M>, IggyError>;
}
