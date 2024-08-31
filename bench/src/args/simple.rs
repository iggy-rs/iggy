use derive_more::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display)]
pub enum BenchmarkKind {
    #[display("send messages")]
    Send,
    #[display("poll messages")]
    Poll,
    #[display("send and poll messages")]
    SendAndPoll,
    #[display("consumer group poll")]
    ConsumerGroupPoll,
}
