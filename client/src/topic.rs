#[derive(Debug)]
pub struct Topic {
    pub id: u32,
    pub partitions: u32,
    pub name: String,
}