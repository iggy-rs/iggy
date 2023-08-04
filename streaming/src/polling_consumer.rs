use iggy::consumer::{Consumer, ConsumerKind};
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PollingConsumer {
    Consumer(u32),
    ConsumerGroup(u32, u32),
}

impl PollingConsumer {
    pub fn from_consumer(consumer: &Consumer, client_id: u32) -> Self {
        match consumer.kind {
            ConsumerKind::Consumer => PollingConsumer::Consumer(consumer.id),
            ConsumerKind::ConsumerGroup => PollingConsumer::ConsumerGroup(consumer.id, client_id),
        }
    }
}

impl Display for PollingConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PollingConsumer::Consumer(consumer_id) => write!(f, "consumer ID: {}", consumer_id),
            PollingConsumer::ConsumerGroup(consumer_group_id, member_id) => {
                write!(
                    f,
                    "consumer group ID: {}, member ID: {}",
                    consumer_group_id, member_id
                )
            }
        }
    }
}
