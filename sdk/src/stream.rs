use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Stream {
    pub id: u32,
    pub topics: u32,
    pub name: String,
}
