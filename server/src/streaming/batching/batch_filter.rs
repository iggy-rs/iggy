use crate::streaming::batching::iterator::IntoMessagesIterator;

pub trait BatchItemizer<M, U: IntoMessagesIterator, T: IntoIterator<Item = U>> {
    fn to_messages(self) -> Vec<M>;
    fn to_messages_with_filter<F>(self, messages_count: usize, f: &F) -> Vec<M>
    where
        F: Fn(&M) -> bool;
}
