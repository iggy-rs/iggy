use derive_more::Display;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Serialize)]
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
