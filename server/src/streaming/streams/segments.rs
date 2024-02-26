use crate::streaming::streams::stream::Stream;
use std::sync::atomic::Ordering;

impl Stream {
    pub fn get_segments_count(&self) -> u32 {
        self.segments_count.load(Ordering::SeqCst)
    }
}
