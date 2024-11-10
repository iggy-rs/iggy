use iggy::utils::byte_size::IggyByteSize;

#[derive(Debug)]
pub struct AppendableBatchInfo {
    pub batch_size: IggyByteSize,
    pub partition_id: u32,
}

impl AppendableBatchInfo {
    pub fn new(batch_size: IggyByteSize, partition_id: u32) -> Self {
        Self {
            batch_size,
            partition_id,
        }
    }
}
