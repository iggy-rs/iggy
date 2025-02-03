use std::{ops::Range, sync::Arc};

use iggy::{
    models::batch::{IggyHeader, IGGY_BATCH_OVERHEAD},
    utils::byte_size::IggyByteSize,
};
use rkyv::util::AlignedVec;

use crate::streaming::local_sizeable::LocalSizeable;

#[derive(Default)]
pub struct IggyBatchSlice {
    pub range: Range<usize>,
    pub bytes: Arc<AlignedVec>,
}

#[derive(Debug)]
pub struct IggyBatchCachePhantom {
    pub header: IggyHeader,
    pub bytes: Arc<AlignedVec>,
}

impl IggyBatchCachePhantom {
    pub fn new(header: IggyHeader, bytes: Arc<AlignedVec>) -> Self {
        Self { header, bytes }
    }
}

impl LocalSizeable for IggyBatchCachePhantom {
    fn get_size_bytes(&self) -> IggyByteSize {
        (IGGY_BATCH_OVERHEAD + self.bytes.len() as u64).into()
    }
}

impl IggyBatchSlice {
    pub fn new(range: Range<usize>, bytes: Arc<AlignedVec>) -> Self {
        Self { range, bytes }
    }
}
#[derive(Default)]
pub struct IggyBatchFetchResult {
    pub batch_slices: Vec<IggyBatchSlice>,
    pub header: IggyHeader,
}

impl IggyBatchFetchResult {
    pub fn new(batch_slices: Vec<IggyBatchSlice>, header: IggyHeader) -> Self {
        Self {
            batch_slices,
            header,
        }
    }
}
