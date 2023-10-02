use crate::streaming::utils::hash;
use iggy::consumer::{Consumer, ConsumerKind};
use iggy::identifier::{IdKind, Identifier};
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PollingConsumer {
    Consumer(u32, u32),      // Consumer ID + Partition ID
    ConsumerGroup(u32, u32), // Consumer Group ID + Member ID
}

impl PollingConsumer {
    pub fn from_consumer(consumer: &Consumer, client_id: u32, partition_id: Option<u32>) -> Self {
        let consumer_id = Self::resolve_consumer_id(&consumer.id);
        match consumer.kind {
            ConsumerKind::Consumer => {
                PollingConsumer::Consumer(consumer_id, partition_id.unwrap_or(0))
            }
            ConsumerKind::ConsumerGroup => PollingConsumer::ConsumerGroup(consumer_id, client_id),
        }
    }

    pub fn resolve_consumer_id(identifier: &Identifier) -> u32 {
        match identifier.kind {
            IdKind::Numeric => identifier.get_u32_value().unwrap(),
            IdKind::String => hash::calculate(&identifier.value),
        }
    }
}

impl Display for PollingConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PollingConsumer::Consumer(consumer_id, partition_id) => write!(
                f,
                "consumer ID: {}, partition ID: {}",
                consumer_id, partition_id
            ),
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
