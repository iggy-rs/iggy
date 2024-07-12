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
    pub fn from_consumer(consumer: &Consumer, client_id: u32, partition_id: &Option<u32>) -> Self {
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
            IdKind::String => hash::calculate_32(&identifier.value),
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

    #[test]
    fn given_consumer_with_numeric_id_polling_consumer_should_be_created() {
        let consumer_id = 1;
        let client_id = 2;
        let partition_id = 3;
        let consumer = Consumer::new(Identifier::numeric(consumer_id).unwrap());
        let polling_consumer =
            PollingConsumer::from_consumer(consumer.clone(), client_id, Some(partition_id));

        assert_eq!(
            polling_consumer,
            PollingConsumer::Consumer(consumer_id, partition_id)
        );

        assert_eq!(
            consumer_id,
            PollingConsumer::resolve_consumer_id(&consumer.id)
        );
    }

    #[test]
    fn given_consumer_with_named_id_polling_consumer_should_be_created() {
        let consumer_name = "consumer";
        let client_id = 2;
        let partition_id = 3;
        let consumer = Consumer::new(Identifier::named(consumer_name).unwrap());
        let polling_consumer =
            PollingConsumer::from_consumer(consumer.clone(), client_id, Some(partition_id));

        let consumer_id = PollingConsumer::resolve_consumer_id(&consumer.id);
        assert_eq!(
            polling_consumer,
            PollingConsumer::Consumer(consumer_id, partition_id)
        );
    }

    #[test]
    fn given_consumer_group_with_numeric_id_polling_consumer_group_should_be_created() {
        let group_id = 1;
        let client_id = 2;
        let consumer = Consumer::group(Identifier::numeric(group_id).unwrap());
        let polling_consumer = PollingConsumer::from_consumer(consumer.clone(), client_id, None);

        assert_eq!(
            polling_consumer,
            PollingConsumer::ConsumerGroup(group_id, client_id)
        );
        assert_eq!(group_id, PollingConsumer::resolve_consumer_id(&consumer.id));
    }

    #[test]
    fn given_consumer_group_with_named_id_polling_consumer_group_should_be_created() {
        let consumer_group_name = "consumer_group";
        let client_id = 2;
        let consumer = Consumer::group(Identifier::named(consumer_group_name).unwrap());
        let polling_consumer = PollingConsumer::from_consumer(consumer.clone(), client_id, None);

        let consumer_id = PollingConsumer::resolve_consumer_id(&consumer.id);
        assert_eq!(
            polling_consumer,
            PollingConsumer::ConsumerGroup(consumer_id, client_id)
        );
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
