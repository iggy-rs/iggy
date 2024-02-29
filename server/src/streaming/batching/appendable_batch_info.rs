#[derive(Debug)]
pub struct AppendableBatchInfo {
    pub batch_size: u64,
    pub partition_id: u32,
}

impl AppendableBatchInfo {
    pub fn new(batch_size: u64, partition_id: u32) -> Self {
        Self {
            batch_size,
            partition_id,
        }
    }
}
