use derive_more::Display;

#[derive(Debug, Clone, Copy, PartialEq, Display)]
pub enum BenchmarkKind {
    #[display(fmt = "send messages")]
    Send,
    #[display(fmt = "poll messages")]
    Poll,
    #[display(fmt = "send and poll messages")]
    SendAndPoll,
}
