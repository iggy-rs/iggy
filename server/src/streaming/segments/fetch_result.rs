use bytes::Bytes;
use iggy::models::batch::IggyHeader;
use std::ops::Range;

#[derive(Default)]
pub struct IggyBatchSlice {
    pub range: Range<usize>,
    pub header: IggyHeader,
    pub bytes: Bytes,
}

impl IggyBatchSlice {
    pub fn new(range: Range<usize>, header: IggyHeader, bytes: Bytes) -> Self {
        Self {
            range,
            header,
            bytes,
        }
    }
}

#[derive(Default)]
pub struct IggyBatchFetchResult {
    pub msg_count: u32,
    pub slices: Vec<IggyBatchSlice>,
    pub header: IggyHeader,
}

impl IggyBatchFetchResult {
    pub fn new(slices: Vec<IggyBatchSlice>, msg_count: u32, header: IggyHeader) -> Self {
        Self {
            slices,
            msg_count,
            header,
        }
    }
}
