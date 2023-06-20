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

impl IndexRange {
    pub fn max_range() -> Self {
        Self {
            start: Index {
                relative_offset: 0,
                position: 0,
            },
            end: Index {
                relative_offset: u32::MAX - 1,
                position: u32::MAX,
            },
        }
    }
}
