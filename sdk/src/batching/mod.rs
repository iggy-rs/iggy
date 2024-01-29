pub mod batcher;
pub mod batches_converter;
pub mod messages_batch;

pub const BATCH_METADATA_BYTES_LEN: u32 = 8 + 4 + 4 + 1;
