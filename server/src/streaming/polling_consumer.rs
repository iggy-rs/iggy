use crate::streaming::utils::hash;
use iggy::identifier::Identifier;
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PollingConsumer {
    Consumer(u32, u32),      // Consumer ID + Partition ID
    ConsumerGroup(u32, u32), // Consumer Group ID + Member ID
}

impl PollingConsumer {
    pub fn consumer(consumer_id: &Identifier, partition_id: u32) -> Self {
        PollingConsumer::Consumer(Self::resolve_consumer_id(consumer_id), partition_id)
    }

    pub fn consumer_group(consumer_group_id: u32, member_id: u32) -> Self {
        PollingConsumer::ConsumerGroup(consumer_group_id, member_id)
    }

    pub fn resolve_consumer_id(identifier: &Identifier) -> u32 {
        match identifier {
            Identifier::Numeric(id) => *id,
            Identifier::String(id) => hash::calculate_32(id.as_bytes()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::consumer::Consumer;

    #[test]
    fn given_consumer_with_numeric_id_polling_consumer_should_be_created() {
        let consumer_id_value = 1;
        let partition_id = 3;
        let consumer_id = Identifier::numeric(consumer_id_value).unwrap();
        let consumer = Consumer::new(consumer_id);
        let polling_consumer = PollingConsumer::consumer(&consumer.id, partition_id);

        assert_eq!(
            polling_consumer,
            PollingConsumer::Consumer(consumer_id_value, partition_id)
        );
    }

    #[test]
    fn given_consumer_with_named_id_polling_consumer_should_be_created() {
        let consumer_name = "consumer";
        let partition_id = 3;
        let consumer_id = Identifier::named(consumer_name).unwrap();
        let consumer = Consumer::new(consumer_id);

        let resolved_consumer_id = PollingConsumer::resolve_consumer_id(&consumer.id);
        let polling_consumer = PollingConsumer::consumer(&consumer.id, partition_id);

        assert_eq!(
            polling_consumer,
            PollingConsumer::Consumer(resolved_consumer_id, partition_id)
        );
    }

    #[test]
    fn given_consumer_group_with_numeric_id_polling_consumer_group_should_be_created() {
        let group_id = 1;
        let client_id = 2;
        let polling_consumer = PollingConsumer::consumer_group(group_id, client_id);

        match polling_consumer {
            PollingConsumer::ConsumerGroup(consumer_group_id, member_id) => {
                assert_eq!(consumer_group_id, group_id);
                assert_eq!(member_id, client_id);
            }
            _ => panic!("Expected ConsumerGroup"),
        }
    }

    #[test]
    fn given_distinct_named_ids_unique_polling_consumer_ids_should_be_created() {
        let name1 = Identifier::named("consumer1").unwrap();
        let name2 = Identifier::named("consumer2").unwrap();
        let id1 = PollingConsumer::resolve_consumer_id(&name1);
        let id2 = PollingConsumer::resolve_consumer_id(&name2);
        assert_ne!(id1, id2);
    }
}
