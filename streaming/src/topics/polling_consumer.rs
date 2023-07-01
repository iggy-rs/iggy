#[derive(Debug, PartialEq)]
pub enum PollingConsumer {
    Consumer(u32),
    Group(u32, u32),
}
