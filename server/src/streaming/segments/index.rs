use crate::streaming::segments::segment::Segment;
use iggy::error::IggyError;
use iggy::error::IggyError::InvalidOffset;

#[derive(Debug, Eq, Clone, Copy, Default)]
pub struct Index {
    pub relative_offset: u32,
    pub position: u32,
}

impl PartialEq<Self> for Index {
    fn eq(&self, other: &Self) -> bool {
        self.relative_offset == other.relative_offset
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IndexRange {
    pub start: Index,
    pub end: Index,
}
impl Segment {
    pub fn load_highest_lower_bound_index(
        &self,
        indices: &[Index],
        start_offset: u32,
        end_offset: u32,
    ) -> Result<IndexRange, IggyError> {
        let starting_offset_idx = binary_search_index(indices, start_offset);
        let ending_offset_idx = binary_search_index(indices, end_offset);

        match (starting_offset_idx, ending_offset_idx) {
            (Some(starting_offset_idx), Some(ending_offset_idx)) => Ok(IndexRange {
                start: indices[starting_offset_idx],
                end: indices[ending_offset_idx],
            }),
            (Some(starting_offset_idx), None) => Ok(IndexRange {
                start: indices[starting_offset_idx],
                end: *indices.last().unwrap(),
            }),
            (None, _) => Err(InvalidOffset(start_offset as u64 + self.start_offset)),
        }
    }
}
fn binary_search_index(indices: &[Index], offset: u32) -> Option<usize> {
    match indices.binary_search_by(|index| index.relative_offset.cmp(&offset)) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::{SegmentConfig, SystemConfig};
    use crate::streaming::storage::tests::get_test_system_storage;
    use iggy::utils::expiry::IggyExpiry;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    fn create_segment() -> Segment {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let start_offset = 0;
        let config = Arc::new(SystemConfig {
            segment: SegmentConfig {
                cache_indexes: true,
                ..Default::default()
            },
            ..Default::default()
        });

        Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            config,
            storage,
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
    }

    fn create_test_indices(segment: &mut Segment) {
        let indexes = vec![
            Index {
                relative_offset: 5,
                position: 0,
            },
            Index {
                relative_offset: 20,
                position: 100,
            },
            Index {
                relative_offset: 35,
                position: 200,
            },
            Index {
                relative_offset: 50,
                position: 300,
            },
            Index {
                relative_offset: 65,
                position: 400,
            },
        ];
        segment.indexes.as_mut().unwrap().extend(indexes);
    }

    #[test]
    fn should_find_both_indices() {
        let mut segment = create_segment();
        create_test_indices(&mut segment);
        let result = segment
            .load_highest_lower_bound_index(segment.indexes.as_ref().unwrap(), 15, 45)
            .unwrap();

        assert_eq!(result.start.relative_offset, 20);
        assert_eq!(result.end.relative_offset, 50);
    }

    #[test]
    fn start_and_end_index_should_be_equal() {
        let mut segment = create_segment();
        create_test_indices(&mut segment);
        let result_end_range = segment
            .load_highest_lower_bound_index(segment.indexes.as_ref().unwrap(), 65, 100)
            .unwrap();

        assert_eq!(result_end_range.start.relative_offset, 65);
        assert_eq!(result_end_range.end.relative_offset, 65);

        let result_start_range = segment
            .load_highest_lower_bound_index(segment.indexes.as_ref().unwrap(), 0, 5)
            .unwrap();
        assert_eq!(result_start_range.start.relative_offset, 5);
        assert_eq!(result_start_range.end.relative_offset, 5);
    }

    #[test]
    fn should_clamp_last_index_when_out_of_range() {
        let mut segment = create_segment();
        create_test_indices(&mut segment);
        let result = segment
            .load_highest_lower_bound_index(segment.indexes.as_ref().unwrap(), 5, 100)
            .unwrap();

        assert_eq!(result.start.relative_offset, 5);
        assert_eq!(result.end.relative_offset, 65);
    }

    #[test]
    fn should_return_err_when_both_indices_out_of_range() {
        let mut segment = create_segment();
        create_test_indices(&mut segment);

        let result =
            segment.load_highest_lower_bound_index(segment.indexes.as_ref().unwrap(), 100, 200);
        assert!(result.is_err());
    }
}
