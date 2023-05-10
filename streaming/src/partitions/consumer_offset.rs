#[derive(Debug)]
pub struct ConsumerOffset {
    pub offset: u64,
    pub path: String,
}
