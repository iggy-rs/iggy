use iggy::utils::timestamp::IggyTimestamp;

#[derive(Debug, Default, Eq, Clone, Copy)]
pub struct TimeIndex {
    pub relative_offset: u32,
    pub timestamp: IggyTimestamp,
}

impl PartialEq<Self> for TimeIndex {
    fn eq(&self, other: &Self) -> bool {
        self.relative_offset == other.relative_offset
            && self.timestamp.to_micros() == other.timestamp.to_micros()
    }
}
