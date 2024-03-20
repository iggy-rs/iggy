use futures::Stream;

pub trait MessageStream {
    type Item;
    fn into_stream(self) -> impl Stream<Item = Self::Item>;
}
