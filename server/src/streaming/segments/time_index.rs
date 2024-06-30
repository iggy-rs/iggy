#[derive(Debug, Default, Eq, Clone, Copy)]
pub struct TimeIndex {
    pub relative_offset: u32,
    pub timestamp: u64,
}

impl PartialEq<Self> for TimeIndex {
    fn eq(&self, other: &Self) -> bool {
        self.relative_offset == other.relative_offset && self.timestamp == other.timestamp
    }
}
