#[derive(Debug)]
pub struct TestArgs {
    pub messages_per_batch: u32,
    pub message_batches: u32,
    pub connections_count: u32,
}
