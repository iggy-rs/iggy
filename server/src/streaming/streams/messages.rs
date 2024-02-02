use crate::streaming::streams::stream::Stream;
use std::sync::atomic::Ordering;

impl Stream {
    pub fn get_messages_count(&self) -> u64 {
        self.messages_count.load(Ordering::SeqCst)
    }
}
