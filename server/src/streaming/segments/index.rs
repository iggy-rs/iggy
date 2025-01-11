use std::ops::Deref;

use crate::streaming::io::{IggyIoUnit, IoUnit};
use crate::streaming::segments::segment::Segment;
use iggy::error::IggyError;
use iggy::error::IggyError::InvalidOffset;

#[derive(Debug, Eq, Clone, Copy, Default)]
pub struct Index {
    pub offset: u32,
    pub position: u32,
    pub timestamp: u64,
}

impl PartialEq<Self> for Index {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

pub trait IndicesToIoVec {
    type IoUnit: IoUnit;
    fn iounit_iter(&self) -> impl Iterator<Item = Self::IoUnit>;
}

impl<T> IndicesToIoVec for T
where
    T: Deref<Target = [Index]>,
{
    type IoUnit = IggyIoUnit;
    fn iounit_iter(&self) -> impl Iterator<Item = Self::IoUnit> {
        self.iter()
            .zip(self.iter().skip(1))
            .map(|(prev, next)| (prev.position as u64, next.position as usize))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IndexRange {
    pub start: Index,
    pub end: Index,
}

impl Segment {
    pub fn load_highest_lower_bound_index<'a>(
        &self,
        indices: &'a [Index],
        start_offset: u32,
        end_offset: u32,
    ) -> Result<&'a [Index], IggyError> {
        let starting_offset_idx = binary_search_index(indices, start_offset);
        let ending_offset_idx = binary_search_index(indices, end_offset);

        match (starting_offset_idx, ending_offset_idx) {
            (Some(starting_offset_idx), Some(ending_offset_idx)) => {
                Ok(&indices[starting_offset_idx..ending_offset_idx])
            }
            (Some(starting_offset_idx), None) => Ok(&indices[starting_offset_idx..]),
            (None, _) => Err(InvalidOffset(start_offset as u64 + self.start_offset)),
        }
    }
}

fn binary_search_index(indices: &[Index], offset: u32) -> Option<usize> {
    match indices.binary_search_by(|index| index.offset.cmp(&offset)) {
        Ok(index) => Some(index),
        Err(index) => {
            if index < indices.len() {
                Some(index)
            } else {
                None
            }
        }
    }
}

impl IndexRange {
    pub fn max_range() -> Self {
        Self {
            start: Index {
                offset: 0,
                position: 0,
                timestamp: 0,
            },
            end: Index {
                offset: u32::MAX - 1,
                position: u32::MAX,
                timestamp: u64::MAX,
            },
        }
    }
}
