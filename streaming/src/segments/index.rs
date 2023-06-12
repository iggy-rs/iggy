#[derive(Debug)]
pub struct Index {
    pub relative_offset: u32,
    pub position: u32,
}

#[derive(Debug)]
pub struct IndexRange {
    pub start: Index,
    pub end: Index,
}
