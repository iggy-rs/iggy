use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PollingConsumer {
    Consumer(u32),
    Group(u32, u32),
}

impl Display for PollingConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PollingConsumer::Consumer(consumer_id) => write!(f, "consumer ID: {}", consumer_id),
            PollingConsumer::Group(group_id, member_id) => {
                write!(
                    f,
                    "consumer group ID: {}, member ID: {}",
                    group_id, member_id
                )
            }
        }
    }
}
